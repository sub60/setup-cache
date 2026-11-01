//! TODO: docs.

use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::fs::File;
use std::io::Write;
use std::os::fd::{FromRawFd, RawFd};
use std::path::PathBuf;
use std::{io, process};

use async_compat::Compat;
use futures::Stream;
use futures::stream::FusedStream;
use smallvec::SmallVec;
use tokio::net::{UnixListener, UnixStream};

#[cfg(not(feature = "sub60-cache"))]
use crate::any_cache::AnyCacheKind;
use crate::event_loop;
use crate::nix_store::NixStore;
#[cfg(feature = "sub60-cache")]
use crate::real_cache::RealCache;
use crate::real_cache::{
    RealCacheConnectError,
    RealCacheRunArgs,
    SUB60_CACHE_URL,
};
use crate::real_context::RealContext;
use crate::real_token_fetcher::{
    NewRealTokenFetcherError,
    RealTokenFetcher,
    RealTokenFetcherArgs,
};
use crate::tokio::{Tokio, TokioSpawner};

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    #[arg(long)]
    socket: PathBuf,

    #[arg(long)]
    ready_fd: Option<RawFd>,

    #[arg(
        long,
        required = false,
        default_value = "",
        value_parser = parse_upstream_caches,
    )]
    pub(crate) upstream_caches: SmallVec<[url::Url; 2]>,

    #[command(flatten)]
    pub(crate) cache_args: RealCacheRunArgs,

    #[command(flatten)]
    token_fetcher_args: RealTokenFetcherArgs,

    #[cfg(not(feature = "sub60-cache"))]
    #[arg(long, value_enum)]
    pub(crate) cache: AnyCacheKind,
}

enum StartError {
    BindSocket(io::Error),
    ConnectToCache(RealCacheConnectError),
    NewTokenFetcher(NewRealTokenFetcherError),
    OpenNixStore(nixb::Error),
}

/// A [`Stream`] of accepted connections from a [`UnixListener`].
struct UnixStreams {
    listener: UnixListener,
}

pub(crate) fn run(args: RunArgs) {
    let tokio = Tokio::new();

    let ready_fd = args.ready_fd;

    let start_res = tokio.block_on(start(args, tokio.spawner()));

    if let Some(ready_fd) = ready_fd {
        let mut ready_file = unsafe { File::from_raw_fd(ready_fd) };

        let mut signal_readiness = || match &start_res {
            Ok(_) => ready_file.write_all(&[0]),
            Err(start_error) => {
                ready_file.write_all(&[1])?;
                ready_file.write_all(start_error.to_string().as_bytes())
            },
        };

        if let Err(io_err) = signal_readiness() {
            eprintln!("Couldn't write to file descriptor {ready_fd}: {io_err}");
        }
    }

    match start_res {
        Ok(run_event_loop) => tokio.block_on(run_event_loop),
        Err(start_error) => {
            if ready_fd.is_none() {
                eprintln!("{start_error}");
            }
            process::exit(1);
        },
    }
}

async fn start(
    args: RunArgs,
    spawner: TokioSpawner,
) -> Result<impl Future<Output = ()> + use<>, StartError> {
    let listener =
        UnixListener::bind(&args.socket).map_err(StartError::BindSocket)?;

    let nix_store = NixStore::open().map_err(StartError::OpenNixStore)?;

    #[cfg(not(feature = "sub60-cache"))]
    let cache = crate::any_cache::AnyCache::new(&args, spawner.clone())
        .await
        .map_err(StartError::ConnectToCache)?;

    #[cfg(feature = "sub60-cache")]
    let cache = RealCache::connect(&args, spawner.clone())
        .await
        .map_err(StartError::ConnectToCache)?;

    let token_fetcher = RealTokenFetcher::new(
        &args.token_fetcher_args,
        SUB60_CACHE_URL.clone(),
    )
    .map_err(StartError::NewTokenFetcher)?;

    let mut ctx = RealContext::new(nix_store, spawner, token_fetcher);

    Ok(async move {
        event_loop::run(args, cache, UnixStreams { listener }, &mut ctx).await;
    })
}

fn parse_upstream_caches(
    value: &str,
) -> Result<SmallVec<[url::Url; 2]>, url::ParseError> {
    value.split(',').filter(|value| !value.is_empty()).map(str::parse).collect()
}

impl Stream for UnixStreams {
    type Item = UnixStream;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.listener.poll_accept(cx) {
                Poll::Ready(Ok((stream, _addr))) => {
                    return Poll::Ready(Some(stream));
                },
                Poll::Ready(Err(err)) => {
                    eprintln!("Couldn't accept stream: {err}");
                    continue;
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl FusedStream for UnixStreams {
    fn is_terminated(&self) -> bool {
        false
    }
}

impl event_loop::Io for tokio::net::UnixStream {
    type Reader = Compat<tokio::net::unix::OwnedReadHalf>;
    type Writer = Compat<tokio::net::unix::OwnedWriteHalf>;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.into_split();
        (Compat::new(reader), Compat::new(writer))
    }
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindSocket(io_error) => {
                write!(f, "Couldn't bind to Unix domain socket: {io_error}")
            },
            Self::ConnectToCache(error) => error.fmt(f),
            Self::NewTokenFetcher(error) => error.fmt(f),
            Self::OpenNixStore(error) => {
                write!(f, "Couldn't open Nix store: {error}")
            },
        }
    }
}
