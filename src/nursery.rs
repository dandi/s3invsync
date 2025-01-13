//! Simple tokio-based task group/nursery
use futures_util::{stream::FuturesUnordered, Stream, StreamExt};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::{JoinError, JoinHandle},
};

/// A handle for spawning new tasks in a task group/nursery.
///
/// `Nursery` is cloneable and sendable, and so it can be used to spawn tasks
/// from inside other tasks in the nursery.  The nursery returned by
/// [`Nursery::new()`] and all clones thereof must be dropped before the
/// corresponding [`NurseryStream`] can yield `None`.
#[derive(Debug)]
pub(crate) struct Nursery<T> {
    sender: UnboundedSender<FragileHandle<T>>,
}

impl<T: Send + 'static> Nursery<T> {
    /// Create a new nursery and return a handle for spawning tasks and a
    /// [`Stream`] of task return values.  `T` is the `Output` type of the
    /// futures that will be spawned in the nursery.
    pub(crate) fn new() -> (Nursery<T>, NurseryStream<T>) {
        let (sender, receiver) = unbounded_channel();
        (
            Nursery { sender },
            NurseryStream {
                receiver,
                tasks: FuturesUnordered::new(),
            },
        )
    }

    /// Spawn a future that returns `T` in the nursery.
    pub(crate) fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = T> + Send + 'static,
    {
        let _ = self.sender.send(FragileHandle::new(tokio::spawn(fut)));
    }
}

// Clone can't be derived, as that would erroneously add `T: Clone` bounds to
// the impl.
impl<T> Clone for Nursery<T> {
    fn clone(&self) -> Nursery<T> {
        Nursery {
            sender: self.sender.clone(),
        }
    }
}

/// A [`Stream`] of the values returned by the tasks spawned in a nursery.
///
/// The corresponding [`Nursery`] and all clones thereof must be dropped before
/// the stream can yield `None`.
///
/// When a `NurseryStream` is dropped, all tasks in the nursery are aborted.
#[derive(Debug)]
pub(crate) struct NurseryStream<T> {
    receiver: UnboundedReceiver<FragileHandle<T>>,
    tasks: FuturesUnordered<FragileHandle<T>>,
}

impl<T: 'static> Stream for NurseryStream<T> {
    type Item = T;

    /// Poll for one of the tasks in the nursery to complete and return its
    /// return value.
    ///
    /// # Panics
    ///
    /// If a task panics, this method resumes unwinding the panic.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let closed = loop {
            match self.receiver.poll_recv(cx) {
                Poll::Pending => break false,
                Poll::Ready(Some(handle)) => self.tasks.push(handle),
                Poll::Ready(None) => break true,
            }
        };
        match ready!(self.tasks.poll_next_unpin(cx)) {
            Some(Ok(r)) => Some(r).into(),
            Some(Err(e)) => match e.try_into_panic() {
                Ok(barf) => std::panic::resume_unwind(barf),
                Err(e) => unreachable!(
                    "Task in nursery should not have been aborted before dropping stream, but got {e:?}"
                ),
            },
            None => {
                if closed {
                    // All Nursery clones dropped and all results yielded; end
                    // of stream
                    None.into()
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

pin_project! {
    /// A wrapper around `tokio::task::JoinHandle` that aborts the task on drop.
    #[derive(Debug)]
    struct FragileHandle<T> {
        #[pin]
        inner: JoinHandle<T>
    }

    impl<T> PinnedDrop for FragileHandle<T> {
        fn drop(this: Pin<&mut Self>) {
            this.project().inner.abort();
        }
    }
}

impl<T> FragileHandle<T> {
    fn new(inner: JoinHandle<T>) -> Self {
        FragileHandle { inner }
    }
}

impl<T> Future for FragileHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.inner.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, StreamExt};
    use std::time::Duration;
    use tokio::{sync::oneshot, time::timeout};

    #[test]
    fn nursery_is_send() {
        #[allow(dead_code)]
        fn require_send<T: Send>(_t: T) {}

        #[allow(dead_code)]
        fn check_nursery_send<T: Send + 'static>() {
            let (nursery, _) = Nursery::<T>::new();
            require_send(nursery);
        }
    }

    #[tokio::test]
    async fn collect() {
        let (nursery, nursery_stream) = Nursery::new();
        nursery.spawn(std::future::ready(1));
        nursery.spawn(std::future::ready(2));
        nursery.spawn(std::future::ready(3));
        drop(nursery);
        let mut values = nursery_stream.collect::<Vec<_>>().await;
        values.sort_unstable();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn nested_spawn() {
        let (nursery, nursery_stream) = Nursery::new();
        let inner = nursery.clone();
        nursery.spawn(async move {
            inner.spawn(std::future::ready(0));
            std::future::ready(1).await
        });
        nursery.spawn(std::future::ready(2));
        nursery.spawn(std::future::ready(3));
        drop(nursery);
        let mut values = nursery_stream.collect::<Vec<_>>().await;
        values.sort_unstable();
        assert_eq!(values, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn reraise_panic() {
        let (nursery, mut nursery_stream) = Nursery::new();
        nursery.spawn(async { panic!("I can't take this anymore!") });
        drop(nursery);
        let r = std::panic::AssertUnwindSafe(nursery_stream.next())
            .catch_unwind()
            .await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn no_close_until_drop() {
        let (nursery, mut nursery_stream) = Nursery::new();
        nursery.spawn(std::future::ready(1));
        nursery.spawn(std::future::ready(2));
        nursery.spawn(std::future::ready(3));
        let mut values = Vec::new();
        values.push(nursery_stream.next().await.unwrap());
        values.push(nursery_stream.next().await.unwrap());
        values.push(nursery_stream.next().await.unwrap());
        values.sort_unstable();
        assert_eq!(values, vec![1, 2, 3]);
        let r = timeout(Duration::from_millis(100), nursery_stream.next()).await;
        assert!(r.is_err());
        drop(nursery);
        let r = timeout(Duration::from_millis(100), nursery_stream.next()).await;
        assert_eq!(r, Ok(None));
    }

    #[tokio::test]
    async fn drop_tasks_on_drop_stream() {
        enum Void {}

        let (nursery, nursery_stream) = Nursery::new();
        let (sender, receiver) = oneshot::channel::<Void>();
        nursery.spawn({
            async move {
                std::future::pending::<()>().await;
                drop(sender);
            }
        });
        drop(nursery);
        drop(nursery_stream);
        assert!(receiver.await.is_err());
    }

    #[tokio::test]
    async fn nest_nurseries() {
        let (nursery, nursery_stream) = Nursery::new();
        nursery.spawn(async {
            let (nursery, nursery_stream) = Nursery::new();
            nursery.spawn(std::future::ready(1));
            nursery.spawn(std::future::ready(2));
            nursery.spawn(std::future::ready(3));
            drop(nursery);
            nursery_stream
                .fold(0, |accum, i| async move { accum + i })
                .await
        });
        nursery.spawn(std::future::ready(4));
        nursery.spawn(std::future::ready(5));
        drop(nursery);
        let mut values = nursery_stream.collect::<Vec<_>>().await;
        values.sort_unstable();
        assert_eq!(values, vec![4, 5, 6]);
    }
}
