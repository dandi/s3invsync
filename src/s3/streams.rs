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

#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub(super) struct ListManifestDates {
    bucket: String,
    key_prefix: String,
    inner: Option<PaginationStream<Result<ListObjectsV2Output, InnerListError>>>,
    results: VecDeque<DateHM>,
}

impl ListManifestDates {
    pub(super) fn new(client: &S3Client, key_prefix: String) -> Self {
        ListManifestDates {
            bucket: client.inv_bucket.clone(),
            key_prefix: key_prefix.clone(),
            inner: Some(
                client
                    .inner
                    .list_objects_v2()
                    .bucket(&client.inv_bucket)
                    .prefix(key_prefix)
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
                        bucket: self.bucket.clone(),
                        prefix: self.key_prefix.clone(),
                        source,
                    }))
                    .into();
                }
            };
            self.results = page
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .filter_map(|CommonPrefix { prefix, .. }| prefix?.parse::<DateHM>().ok())
                .collect::<VecDeque<_>>();
        }
    }
}

#[derive(Debug, Error)]
#[error("failed to list S3 objects in bucket {bucket:?} with prefix {prefix:?}")]
pub(crate) struct ListObjectsError {
    bucket: String,
    prefix: String,
    source: InnerListError,
}
