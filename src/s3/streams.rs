use super::location::S3Location;
use super::S3Client;
use crate::timestamps::DateHM;
use aws_sdk_s3::{
    operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output},
    types::CommonPrefix,
};
use aws_smithy_async::future::pagination_stream::PaginationStream;
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use futures_util::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use thiserror::Error;

type InnerListError = SdkError<ListObjectsV2Error, HttpResponse>;

/// A [`Stream`] that paginates over S3 directories with a given prefix and
/// parses their names as [`DateHM`] values, yielding the successful parses.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub(crate) struct ListManifestDates {
    url: S3Location,
    inner: Option<PaginationStream<Result<ListObjectsV2Output, InnerListError>>>,
    results: VecDeque<DateHM>,
}

impl ListManifestDates {
    /// Construct a new `ListManifestDates` that uses `client` to paginate over
    /// directories that have the prefix given by `url`.
    pub(super) fn new(client: &S3Client, url: &S3Location) -> Self {
        ListManifestDates {
            url: url.clone(),
            inner: Some(
                client
                    .inner
                    .list_objects_v2()
                    .bucket(url.bucket())
                    .prefix(url.key())
                    .delimiter("/")
                    .into_paginator()
                    .send(),
            ),
            results: VecDeque::new(),
        }
    }
}

impl Stream for ListManifestDates {
    type Item = Result<DateHM, ListObjectsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(d) = self.results.pop_front() {
                return Some(Ok(d)).into();
            }
            let Some(inner) = self.inner.as_mut() else {
                return None.into();
            };
            let Some(r) = ready!(inner.poll_next(cx)) else {
                self.inner = None;
                return None.into();
            };
            let page = match r {
                Ok(page) => page,
                Err(source) => {
                    self.inner = None;
                    return Some(Err(ListObjectsError {
                        url: self.url.clone(),
                        source,
                    }))
                    .into();
                }
            };
            self.results = page
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .filter_map(|CommonPrefix { prefix, .. }| {
                    prefix?
                        .strip_suffix('/')?
                        .rsplit_once('/')
                        .map(|(_, s)| s)?
                        .parse::<DateHM>()
                        .ok()
                })
                .collect::<VecDeque<_>>();
        }
    }
}

/// Error yielded by [`ListManifestDates`] when a "List Objects V2" request
/// fails
#[derive(Debug, Error)]
#[error("failed to list S3 objects in {url}")]
pub(crate) struct ListObjectsError {
    url: S3Location,
    source: InnerListError,
}
