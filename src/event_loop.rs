use core::num::NonZeroU64;
use core::pin::{Pin, pin};
use core::task::{Context as TaskContext, Poll};
use core::time::Duration;
use std::collections::{HashSet, VecDeque};
use std::io;

use async_compression::futures::write::ZstdEncoder;
use futures::io::{BufWriter, Cursor};
use futures::stream::{FusedStream, FuturesUnordered};
use futures::{
    AsyncRead,
    AsyncSeekExt,
    AsyncWrite,
    AsyncWriteExt,
    FutureExt,
    StreamExt,
    future,
    select,
};
use nix_types::{
    CompressionAlgorithm,
    NarFileName,
    NarInfo,
    NarInfoFileName,
    Nix32Digest,
    StorePath,
};
use pin_project_lite::pin_project;
use sha2::{Digest, Sha256};

use crate::context::{
    Cache,
    Context,
    HttpClient,
    JoinHandle,
    Nix,
    Spawner,
    StopProgressReporter,
    StorePathInfos,
    TokenFetcher,
};
use crate::protocol::{self, StoreDir};
use crate::run::RunArgs;
use crate::token::{self, SharedToken, Token};

/// Maximum number of store paths handled concurrently.
const MAX_CONCURRENT_STORE_PATHS: usize = 32;

/// How long before the token expires it's replaced with a new one.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_mins(5);

/// Target upper bound for memory occupied by all in-memory NAR buffers.
const MAX_IN_MEMORY_NAR_BUFFERS_SIZE: usize = 256 * 1024 * 1024;

/// The algorithm used to compress NARs uploaded to the cache.
const NAR_COMPRESSION_ALGORITHM: CompressionAlgorithm =
    CompressionAlgorithm::Zstd;

const NAR_TEMPORARY_FILE_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) trait Io: Send + 'static {
    type Reader: AsyncRead + Send;
    type Writer: AsyncWrite + Send;
    fn split(self) -> (Self::Reader, Self::Writer);
}

pub(crate) struct ActionReport<Ctx: Context> {
    /// The number of bytes pushed to the cache.
    pub(crate) num_bytes_pushed: u64,

    /// The number of store paths pushed to the cache.
    pub(crate) num_paths_pushed: u32,

    /// The number of store paths already stored in the cache
    pub(crate) num_paths_skipped: u32,

    /// The error refreshing the token, if any.
    pub(crate) token_refresh_error:
        Option<<Ctx::TokenFetcher as TokenFetcher>::Error>,

    /// The errors asking the cache which store paths it doesn't have, each with
    /// the number of store paths that weren't handled because of it.
    pub(crate) narinfo_lookup_errors: Vec<(u32, <Ctx::Cache as Cache>::Error)>,

    /// The list of store paths for which it wasn't possible to compute their
    /// closure, together with the corresponding error.
    pub(crate) path_closure_errors:
        Vec<(StorePath<StoreDir>, <Ctx::Nix as Nix>::StoreClosureError)>,

    /// The list of store paths for which it wasn't possible to compute their
    /// closure, together with the corresponding error.
    #[expect(clippy::type_complexity)]
    pub(crate) path_handling_errors:
        Vec<(StorePath<StoreDir>, HandlePathError<Ctx::Cache, Ctx::Nix>)>,
}

/// The type of error that can occur when [handling](handle_store_path) a store
/// path.
#[derive(cauchy::Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum HandlePathError<C: Cache, N: Nix> {
    #[display("couldn't create temporary file to read NAR into")]
    CreateTempFile(#[source] io::Error),
    #[display("couldn't finish compressing NAR")]
    FinishNarCompression(#[source] io::Error),
    #[display("couldn't get store path infos")]
    GetPathInfos(#[source] N::PathInfosError),
    #[display("couldn't read NAR from store")]
    ReadNar(#[source] N::ReadNarError),
    #[display("read zero bytes from {_0}'s NAR")]
    ReadZeroNarBytes(StorePath<StoreDir>),
    #[display("couldn't rewind NAR buffer's cursor")]
    RewindNarBuffer(#[source] io::Error),
    #[display("couldn't upload NAR to cache")]
    UploadNar(#[source] C::Error),
    #[display("couldn't upload NARInfo to cache")]
    UploadNarInfo(#[source] C::Error),
}

enum Event<Writer> {
    PushStorePath(StorePath<StoreDir>),
    Stop(Writer),
}

pin_project! {
    /// An [`AsyncWrite`] adapter which maintains a running SHA-256 hash of the
    /// written bytes and counts how many bytes were successfully written.
    struct HashAndCountWriter<W> {
        #[pin]
        inner: W,
        sha256: Sha256,
        num_bytes_written: u64,
    }
}

impl<W> HashAndCountWriter<W> {
    fn new(writer: W) -> Self {
        Self { inner: writer, sha256: Sha256::new(), num_bytes_written: 0 }
    }
}

/// Runs the daemon's event loop, pushing the store paths it receives with the
/// given token, which it refreshes with the given fetcher before it expires,
/// writing it to the netrc file each time.
pub(crate) async fn run<Ctx: Context>(
    args: RunArgs,
    cache: Ctx::Cache,
    token: Token,
    token_expiration: Option<Duration>,
    token_fetcher: Ctx::TokenFetcher,
    mut io_stream: impl FusedStream<Item = impl Io> + Unpin,
    ctx: &mut Ctx,
) {
    let (message_tx, message_rx) = flume::unbounded();

    let token = SharedToken::new(token);
    let mut token_refresh = pin!(future::Fuse::terminated());

    if let Some(expires_in) = token_expiration {
        token_refresh.set(refresh_token(&token_fetcher, expires_in).fuse());
    }

    let mut store_closures = FuturesUnordered::new();
    let mut narinfo_lookups = FuturesUnordered::new();
    let mut handle_store_paths = FuturesUnordered::new();
    let mut pending_handle_store_paths = VecDeque::new();
    let mut seen_store_hashes = HashSet::new();

    let mut report = ActionReport::<Ctx>::default();
    let mut stop_progress_writer = None;

    let http_client = ctx.http_client().clone();
    let nix = ctx.nix().clone();

    let handle_store_path_fut =
        |path: StorePath<StoreDir>, token: SharedToken| {
            let mut upstream_caches = args.upstream_caches.clone();
            let cache = cache.clone();
            let http_client = http_client.clone();
            let mut nix = nix.clone();
            async move {
                let result = handle_store_path(
                    &path,
                    &token,
                    &mut upstream_caches,
                    &cache,
                    &http_client,
                    &mut nix,
                )
                .await;
                (result, path)
            }
        };

    let lookup_missing_narinfos_fut =
        |paths: Vec<StorePath<StoreDir>>, token: SharedToken| {
            let cache = cache.clone();
            async move {
                let store_hashes =
                    paths.iter().map(|path| *path.hash()).collect::<Vec<_>>();
                let result =
                    cache.missing_narinfos(&token, &store_hashes).await;
                (result, paths, token)
            }
        };

    loop {
        // The store paths being looked up are handled once the cache answers.
        if stop_progress_writer.is_some()
            && store_closures.is_empty()
            && narinfo_lookups.is_empty()
        {
            break;
        }

        select! {
            io = io_stream.select_next_some() => {
                ctx.spawner().spawn(handle_io(io, message_tx.clone())).detach();
            },
            message_res = message_rx.recv_async() => {
                match message_res.expect("message_tx is still alive") {
                    Ok(Event::PushStorePath(store_path)) => {
                        let mut nix = ctx.nix().clone();
                        let fut = async move {
                            nix.store_closure(&store_path)
                                .await
                                .map_err(|err| (store_path, err))
                        };
                        store_closures.push(ctx.spawner().spawn(fut));
                    },
                    Ok(Event::Stop(writer)) => {
                        stop_progress_writer = Some(writer);
                    },
                    Err(rx_err) => ctx.handle_rx_error(rx_err),
                }
            },
            closure_res = store_closures.select_next_some() => {
                match closure_res {
                    Ok(store_paths) => {
                        // Skip handling the paths we've already seen.
                        let new_paths = store_paths
                            .into_iter()
                            .filter(|path| seen_store_hashes.insert(*path.hash()))
                            .collect::<Vec<_>>();

                        if new_paths.is_empty() {
                            continue;
                        }

                        let fut =
                            lookup_missing_narinfos_fut(new_paths, token.clone());
                        narinfo_lookups.push(ctx.spawner().spawn(fut));
                    },
                    Err(tuple) => report.path_closure_errors.push(tuple),
                }
            },
            refresh = token_refresh => {
                match refresh {
                    Ok((new_token, expires_in)) => {
                        token::write_token(&new_token, &args.origin, &args.netrc_file);
                        token.update(new_token);
                        if let Some(expires_in) = expires_in {
                            token_refresh.set(
                                refresh_token(&token_fetcher, expires_in).fuse(),
                            );
                        }
                    },
                    // The old token keeps being used until it expires.
                    Err(err) => report.token_refresh_error = Some(err),
                }
            },
            (result, paths, token) = narinfo_lookups.select_next_some() => {
                let mut missing = match result {
                    Ok(missing) => missing.into_iter().peekable(),
                    Err(err) => {
                        report.narinfo_lookup_errors.push((paths.len() as u32, err));
                        continue;
                    },
                };

                for path in paths {
                    if missing.next_if_eq(path.hash()).is_none() {
                        report.num_paths_skipped += 1;
                        continue;
                    }

                    let fut = handle_store_path_fut(path, token.clone());

                    if handle_store_paths.len() < MAX_CONCURRENT_STORE_PATHS {
                        handle_store_paths.push(ctx.spawner().spawn(fut));
                    } else {
                        pending_handle_store_paths.push_back(fut);
                    }
                }
            },
            (result, path) = handle_store_paths.select_next_some() => {
                match result {
                    Ok(Some(num_bytes)) => {
                        report.num_bytes_pushed += u64::from(num_bytes);
                        report.num_paths_pushed += 1;
                    },
                    Ok(None) => report.num_paths_skipped += 1,
                    Err(err) => report.path_handling_errors.push((path, err)),
                }

                if let Some(fut) = pending_handle_store_paths.pop_front() {
                    handle_store_paths.push(ctx.spawner().spawn(fut));
                }
            },
        };
    }

    let writer = pin!(stop_progress_writer.expect("received a stop request"));

    let mut reporter = ctx.new_stop_progress_reporter(writer);

    reporter
        .report_paths_left_to_handle(
            (handle_store_paths.len() + pending_handle_store_paths.len())
                as u32,
        )
        .await;

    while let Some((result, path)) = handle_store_paths.next().await {
        match result {
            Ok(Some(num_bytes)) => {
                reporter.report_path_pushed(&path, num_bytes).await;
                report.num_bytes_pushed += u64::from(num_bytes);
                report.num_paths_pushed += 1;
            },
            Ok(None) => {
                reporter.report_path_skipped(&path).await;
                report.num_paths_skipped += 1;
            },
            Err(err) => {
                reporter.report_path_handling_error(&path, &err).await;
                report.path_handling_errors.push((path, err))
            },
        }

        if let Some(fut) = pending_handle_store_paths.pop_front() {
            handle_store_paths.push(ctx.spawner().spawn(fut));
        }
    }

    reporter.report_final_report(report).await;
}

async fn handle_io<I: Io>(
    io: I,
    message_tx: flume::Sender<Result<Event<I::Writer>, protocol::ReceiveError>>,
) {
    let (reader, writer) = io.split();
    let reader = pin!(reader);
    let mut message_rx = protocol::Receiver::new(reader);
    loop {
        let Some(message_res) = message_rx.next().await else { return };
        match message_res {
            Ok(protocol::Message::PushStorePath(store_path)) => {
                let _ = message_tx.send(Ok(Event::PushStorePath(store_path)));
            },
            Ok(protocol::Message::StopDaemon) => {
                let _ = message_tx.send(Ok(Event::Stop(writer)));
                return;
            },
            Err(err) => {
                let _ = message_tx.send(Err(err));
            },
        }
    }
}

/// Fetches a new token shortly before the one valid for the given duration
/// expires.
async fn refresh_token<F: TokenFetcher>(
    token_fetcher: &F,
    expires_in: Duration,
) -> Result<(Token, Option<Duration>), F::Error> {
    // A token valid for less than the margin is refreshed halfway through
    // instead of right away, so it's not refreshed in a loop.
    tokio::time::sleep(
        expires_in.saturating_sub(TOKEN_REFRESH_MARGIN).max(expires_in / 2),
    )
    .await;

    token_fetcher.fetch().await
}

/// Handles the given store path by uploading its NAR (compressed) and NARInfo,
/// unless an upstream cache has it.
///
/// Returns the total number of bytes pushed to the cache, or `None` if an
/// upstream cache has the path.
async fn handle_store_path<C: Cache, H: HttpClient, N: Nix>(
    store_path: &StorePath<StoreDir>,
    token: &SharedToken,
    upstream_caches: &mut [url::Url],
    cache: &C,
    http_client: &H,
    nix: &mut N,
) -> Result<Option<NonZeroU64>, HandlePathError<C, N>> {
    let narinfo_filename = NarInfoFileName { store_hash: *store_path.hash() };

    if upstream_caches_have_narinfo(
        &narinfo_filename,
        upstream_caches,
        http_client,
    )
    .await
    {
        return Ok(None);
    }

    let mut infos = nix
        .get_path_infos(store_path)
        .await
        .map_err(HandlePathError::GetPathInfos)?;

    let nar_size = infos.nar_size().map_err(HandlePathError::GetPathInfos)?;

    // Small NARs are written to and read from in-memory buffers, while larger
    // ones are buffered in temporary files to keep peak memory usage bounded.
    let nar_buf = if nar_size.get()
        <= (MAX_IN_MEMORY_NAR_BUFFERS_SIZE / MAX_CONCURRENT_STORE_PATHS) as u64
    {
        future::Either::Left(Cursor::new(Vec::new()))
    } else {
        let file = tempfile::tempfile()
            .map(tokio::fs::File::from_std)
            .map(async_compat::Compat::new)
            .map(|f| {
                BufWriter::with_capacity(NAR_TEMPORARY_FILE_BUFFER_SIZE, f)
            })
            .map_err(HandlePathError::CreateTempFile)?;
        future::Either::Right(file)
    };

    let mut nar_encoder = ZstdEncoder::new(HashAndCountWriter::new(nar_buf));

    nix.read_nar(store_path, &mut nar_encoder)
        .await
        .map_err(HandlePathError::ReadNar)?;

    nar_encoder.close().await.map_err(HandlePathError::FinishNarCompression)?;

    let writer = nar_encoder.into_inner();

    let file_hash = Nix32Digest::new(&writer.sha256.finalize().into());

    let file_size = NonZeroU64::new(writer.num_bytes_written)
        .ok_or_else(|| HandlePathError::ReadZeroNarBytes(store_path.clone()))?;

    let nar_file_name = NarFileName {
        file_hash,
        extension: NAR_COMPRESSION_ALGORITHM.extension(),
    };

    let mut nar_buf = writer.inner;

    // Rewind the buffer's cursor to let the cache read its contents from the
    // beginning.
    nar_buf
        .seek(io::SeekFrom::Start(0))
        .await
        .map_err(HandlePathError::RewindNarBuffer)?;

    // Just like `nix copy --to`, we write the NAR *before* the NARInfo to avoid
    // the cache server temporarily reporting false positives.

    let nar_size_pushed = cache
        .upload_nar(token, nar_file_name, file_size, nar_buf)
        .await
        .map_err(HandlePathError::UploadNar)?;

    let narinfo = || -> Result<_, N::PathInfosError> {
        Ok(NarInfo {
            store_path: store_path.clone(),
            url: (),
            compression: NAR_COMPRESSION_ALGORITHM,
            file_hash,
            file_size,
            nar_hash: infos.nar_hash()?,
            nar_size,
            references: infos.references()?.into_iter().collect(),
            deriver: infos.deriver()?,
            signatures: infos.signatures()?.into_iter().collect(),
            content_address: infos.content_address()?,
        })
    }()
    .map_err(HandlePathError::GetPathInfos)?;

    let narinfo_size = cache
        .upload_narinfo(token, narinfo_filename, narinfo)
        .await
        .map_err(HandlePathError::UploadNarInfo)?;

    Ok(NonZeroU64::new(nar_size_pushed.saturating_add(narinfo_size)))
}

async fn upstream_caches_have_narinfo<H: HttpClient>(
    narinfo_filename: &NarInfoFileName,
    upstream_caches: &mut [url::Url],
    http_client: &H,
) -> bool {
    let narinfo_path_segment = narinfo_filename.to_string();

    for upstream_cache in upstream_caches.iter_mut() {
        upstream_cache
            .path_segments_mut()
            .expect("upstream cache URL can be a base")
            .push(&narinfo_path_segment);
    }

    let mut requests = FuturesUnordered::new();

    for upstream_cache in upstream_caches.iter().cloned() {
        requests.push(async {
            matches!(
                http_client.get(upstream_cache).await,
                Ok(http::StatusCode::OK)
            )
        });
    }

    while let Some(has_narinfo) = requests.next().await {
        if has_narinfo {
            return true;
        }
    }

    false
}

impl<Ctx: Context> Default for ActionReport<Ctx> {
    fn default() -> Self {
        Self {
            num_bytes_pushed: 0,
            num_paths_pushed: 0,
            num_paths_skipped: 0,
            token_refresh_error: None,
            narinfo_lookup_errors: Default::default(),
            path_closure_errors: Default::default(),
            path_handling_errors: Default::default(),
        }
    }
}

impl<W: AsyncWrite> AsyncWrite for HashAndCountWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        match this.inner.poll_write(cx, buf) {
            Poll::Ready(Ok(num_bytes)) => {
                this.sha256.update(&buf[..num_bytes]);
                *this.num_bytes_written += num_bytes as u64;
                Poll::Ready(Ok(num_bytes))
            },
            other => other,
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().inner.poll_close(cx)
    }
}
