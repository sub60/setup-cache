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
use crate::context::TokenFetcher as _;
use crate::nix_store::NixStore;
#[cfg(feature = "sub60-cache")]
use crate::real_cache::RealCache;
use crate::real_cache::{RealCacheConnectError, RealCacheRunArgs};
use crate::real_context::RealContext;
use crate::real_stop_progress_reporter::WithCauses;
use crate::real_token_fetcher::{
    FetchTokenError,
    NewRealTokenFetcherError,
    RealTokenFetcher,
    RealTokenFetcherArgs,
};
use crate::tokio::{Tokio, TokioSpawner};
use crate::{event_loop, token};

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    #[arg(long)]
    socket: PathBuf,

    #[arg(long)]
    ready_fd: Option<RawFd>,

    /// The netrc file to write the token to, which Nix reads to authenticate
    /// its requests to the cache.
    #[arg(long)]
    pub(crate) netrc_file: PathBuf,

    /// The comma-separated URLs of the upstream binary caches.
    #[arg(long, value_parser = parse_upstream_caches)]
    pub(crate) upstream_caches: Option<SmallVec<[url::Url; 2]>>,

    /// The origin of the cache's server, e.g. `https://cache.sub60.dev`.
    #[arg(long, value_parser = parse_origin)]
    pub(crate) origin: url::Url,

    #[command(flatten)]
    pub(crate) cache_args: RealCacheRunArgs,

    #[command(flatten)]
    token_fetcher_args: RealTokenFetcherArgs,

    #[cfg(not(feature = "sub60-cache"))]
    #[arg(long, value_enum)]
    pub(crate) cache_kind: AnyCacheKind,
}

/// How the daemon started, when nothing went wrong setting it up.
enum Started<EventLoop> {
    /// The daemon got the token, and runs the given event loop.
    Running(EventLoop),

    /// The daemon couldn't get the token, so there's nothing to push store
    /// paths with and it doesn't run.
    WithoutToken(FetchTokenError),
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
            Ok(Started::Running(_)) => ready_file.write_all(&[0]),
            Ok(Started::WithoutToken(fetch_error)) => {
                ready_file.write_all(&[2])?;
                ready_file.write_all(
                    WithCauses::Inline(fetch_error).to_string().as_bytes(),
                )
            },
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
        Ok(Started::Running(run_event_loop)) => tokio.block_on(run_event_loop),
        Ok(Started::WithoutToken(fetch_error)) => {
            if ready_fd.is_none() {
                eprintln!(
                    "Couldn't get a token: {}",
                    WithCauses::Inline(&fetch_error)
                );
            }
            process::exit(1);
        },
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
) -> Result<Started<impl Future<Output = ()> + use<>>, StartError> {
    let listener =
        UnixListener::bind(&args.socket).map_err(StartError::BindSocket)?;

    let nix_store = NixStore::open().map_err(StartError::OpenNixStore)?;

    #[cfg(not(feature = "sub60-cache"))]
    let cache = crate::any_cache::AnyCache::new(&args)
        .map_err(StartError::ConnectToCache)?;

    #[cfg(feature = "sub60-cache")]
    let cache = RealCache::new(&args).map_err(StartError::ConnectToCache)?;

    let token_fetcher =
        RealTokenFetcher::new(&args.token_fetcher_args, args.origin.clone())
            .map_err(StartError::NewTokenFetcher)?;

    let (token, expiration) = match token_fetcher.fetch().await {
        Ok(fetched_token) => fetched_token,
        Err(fetch_error) => return Ok(Started::WithoutToken(fetch_error)),
    };

    token::write_token(&token, &args.origin, &args.netrc_file);

    let mut ctx = RealContext::new(nix_store, spawner);

    Ok(Started::Running(async move {
        event_loop::run(
            args,
            cache,
            token,
            expiration,
            token_fetcher,
            UnixStreams { listener },
            &mut ctx,
        )
        .await;
    }))
}

fn parse_upstream_caches(
    value: &str,
) -> Result<SmallVec<[url::Url; 2]>, url::ParseError> {
    value.split(',').filter(|value| !value.is_empty()).map(str::parse).collect()
}

fn parse_origin(value: &str) -> Result<url::Url, String> {
    let url = value.parse::<url::Url>().map_err(|err| err.to_string())?;

    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(
            "must be an origin, without a path, query or fragment".into()
        );
    }

    Ok(url)
}

impl RunArgs {
    /// Returns the URL of the cache.
    pub(crate) fn cache_url(&self) -> url::Url {
        let mut cache_url = self.origin.clone();
        cache_url
            .path_segments_mut()
            .expect("cache URL can be a base")
            .push(&self.cache_args.namespace)
            .push(&self.cache_args.cache);
        cache_url
    }
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
