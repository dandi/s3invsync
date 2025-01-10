use futures_util::{FutureExt, Stream};
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

type UnwindResult<T> = Result<T, Box<dyn std::any::Any + Send>>;

#[derive(Debug)]
pub(crate) struct Nursery<T> {
    sender: UnboundedSender<UnwindResult<T>>,
}

impl<T: Send + 'static> Nursery<T> {
    pub(crate) fn new() -> (Nursery<T>, NurseryStream<T>) {
        let (sender, receiver) = unbounded_channel();
        (Nursery { sender }, NurseryStream { receiver })
    }

    pub(crate) fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = T> + Send + 'static,
    {
        let sender = self.sender.clone();
        tokio::spawn(async move {
            let task = std::panic::AssertUnwindSafe(fut).catch_unwind();
            let _ = sender.send(task.await);
        });
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

#[derive(Debug)]
pub(crate) struct NurseryStream<T> {
    receiver: UnboundedReceiver<UnwindResult<T>>,
}

impl<T: 'static> Stream for NurseryStream<T> {
    type Item = T;

    /// Poll for one of the tasks in the group to complete and return its
    /// return value.
    ///
    /// # Panics
    ///
    /// If a task panics, this method resumes unwinding the panic.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match ready!(self.receiver.poll_recv(cx)) {
            Some(Ok(r)) => Some(r).into(),
            Some(Err(e)) => std::panic::resume_unwind(e),
            None => None.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

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
}
