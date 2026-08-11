use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::panic;

use pin_project_lite::pin_project;

use crate::context;
use crate::context::{JoinHandle as _, Spawner as _};

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

/// Future returned by the
/// [`run_to_completion`](TokioSpawner::run_to_completion) method.
pub(crate) struct RunToCompletion<Fut>
where
    Fut: Future + Send + Unpin + 'static,
    Fut::Output: Send + 'static,
{
    future: Option<Fut>,
    spawner: TokioSpawner,
    spawn_on_drop: bool,
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

impl TokioSpawner {
    /// Returns a wrapper around the given future which, if dropped after it
    /// returns [`Poll::Pending`], moves it to a newly spawned task and polls it
    /// to completion. Completed and never-polled futures are dropped normally.
    pub(crate) fn run_to_completion<Fut>(
        &self,
        future: Fut,
    ) -> RunToCompletion<Fut>
    where
        Fut: Future + Send + Unpin + 'static,
        Fut::Output: Send + 'static,
    {
        RunToCompletion {
            future: Some(future),
            spawner: self.clone(),
            spawn_on_drop: false,
        }
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

impl<Fut> Future for RunToCompletion<Fut>
where
    Fut: Future + Send + Unpin + 'static,
    Fut::Output: Send + 'static,
{
    type Output = Fut::Output;

    fn poll(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();

        // Reset it to false before every poll to avoid respawning a future
        // whose poll panicked.
        this.spawn_on_drop = false;

        let result = Pin::new(
            this.future.as_mut().expect("always present until dropped"),
        )
        .poll(ctx);

        match result {
            Poll::Ready(output) => Poll::Ready(output),
            Poll::Pending => {
                this.spawn_on_drop = true;
                Poll::Pending
            },
        }
    }
}

impl<Fut> Drop for RunToCompletion<Fut>
where
    Fut: Future + Send + Unpin + 'static,
    Fut::Output: Send + 'static,
{
    fn drop(&mut self) {
        if self.spawn_on_drop {
            let future = self.future.take().expect("only taken on Drop");
            self.spawner.spawn(future).detach();
        }
    }
}
