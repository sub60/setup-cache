use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::panic;

use pin_project_lite::pin_project;

use crate::context;

pub(crate) struct Tokio {
    inner: tokio::runtime::Runtime,
}

#[derive(Clone)]
pub(crate) struct TokioSpawner {
    inner: tokio::runtime::Handle,
}

pin_project! {
    pub(crate) struct TokioJoinHandle<T> {
        #[pin]
        inner: tokio::task::JoinHandle<T>,
    }
}

impl Tokio {
    pub(crate) fn block_on<Fut: Future>(&self, future: Fut) -> Fut::Output {
        self.inner.block_on(future)
    }

    #[track_caller]
    pub(crate) fn new() -> Self {
        tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build()
            .map(|inner| Self { inner })
            .expect("couldn't create tokio runtime")
    }

    pub(crate) fn spawner(&self) -> TokioSpawner {
        TokioSpawner { inner: self.inner.handle().clone() }
    }
}

impl context::Spawner for TokioSpawner {
    type JoinHandle<Fut: Future> = TokioJoinHandle<Fut::Output>;

    fn spawn<Fut>(&self, future: Fut) -> Self::JoinHandle<Fut>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        TokioJoinHandle { inner: self.inner.spawn(future) }
    }
}

impl<T> Future for TokioJoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.project().inner.poll(ctx)) {
            Ok(output) => Poll::Ready(output),
            Err(join_err) => panic::resume_unwind(join_err.into_panic()),
        }
    }
}

impl<T> context::JoinHandle<T> for TokioJoinHandle<T> {
    fn detach(self) {}
}
