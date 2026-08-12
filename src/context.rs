use core::error::Error;
use core::num::NonZeroU64;

use futures::{AsyncRead, AsyncWrite};
use nix_types::{
    ContentAddress,
    NarFileName,
    NarInfo,
    NarInfoFileName,
    Nix32Digest,
    Signature,
    StoreBasename,
    StorePath,
};

use crate::event_loop;
use crate::protocol::{self, StoreDir};

pub trait Context {
    type Cache: Cache;
    type HttpClient: HttpClient;
    type Nix: Nix;
    type Spawner: Spawner;
    type StopProgressReporter<W: AsyncWrite + Unpin>: StopProgressReporter;

    fn handle_rx_error(&mut self, rx_error: protocol::ReceiveError);

    fn http_client(&self) -> &Self::HttpClient;

    fn new_stop_progress_reporter<W: AsyncWrite + Unpin>(
        &mut self,
        writer: W,
    ) -> Self::StopProgressReporter<W>;

    fn nix(&self) -> &Self::Nix;

    fn spawner(&self) -> &Self::Spawner;
}

pub trait Cache: Clone + Send + Sync + 'static {
    type Error: Error + Send;

    fn has_narinfo(
        &self,
        narinfo_filename: &NarInfoFileName,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Uploads a NAR and returns the number of bytes added to the cache. Returns
    /// zero if an equivalent NAR was already present.
    fn upload_nar(
        &self,
        nar_file_name: NarFileName,
        nar_size: NonZeroU64,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Uploads a NARInfo and returns the number of bytes added to the cache.
    /// Returns zero if an equivalent NARInfo was already present.
    fn upload_narinfo(
        &self,
        narinfo_filename: NarInfoFileName,
        narinfo: NarInfo<(), StoreDir>,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}

pub trait HttpClient: Clone + Send + Sync + 'static {
    type Error: Error + Send;

    fn get(
        &self,
        url: url::Url,
    ) -> impl Future<Output = Result<http::StatusCode, Self::Error>> + Send;
}

pub trait StopProgressReporter {
    fn report_paths_left_to_handle(
        &mut self,
        num_paths_left_to_handle: u32,
    ) -> impl Future<Output = ()>;

    fn report_path_pushed(
        &mut self,
        path: &StorePath<StoreDir>,
        num_bytes: NonZeroU64,
    ) -> impl Future<Output = ()>;

    fn report_path_skipped(
        &mut self,
        path: &StorePath<StoreDir>,
    ) -> impl Future<Output = ()>;

    fn report_path_handling_error<C: Cache, N: Nix>(
        &mut self,
        path: &StorePath<StoreDir>,
        error: &event_loop::HandlePathError<C, N>,
    ) -> impl Future<Output = ()>;

    fn report_final_report<Ctx: Context>(
        self,
        report: event_loop::ActionReport<Ctx>,
    ) -> impl Future<Output = ()>;
}

pub trait Nix: Clone + Send + 'static {
    type PathInfos<'path>: StorePathInfos<Error = Self::PathInfosError> + Send;

    type PathInfosError: Error + Send;
    type ReadNarError: Error + Send;
    type StoreClosureError: Error + Send;

    fn get_path_infos<'path>(
        &mut self,
        store_path: &'path StorePath<StoreDir>,
    ) -> impl Future<
        Output = Result<Self::PathInfos<'path>, Self::PathInfosError>,
    > + Send;

    fn read_nar(
        &mut self,
        store_path: &StorePath<StoreDir>,
        buf: &mut (impl AsyncWrite + Unpin + Send),
    ) -> impl Future<Output = Result<(), Self::ReadNarError>> + Send;

    fn store_closure(
        &mut self,
        store_path: &StorePath<StoreDir>,
    ) -> impl Future<
        Output = Result<Vec<StorePath<StoreDir>>, Self::StoreClosureError>,
    > + Send;
}

pub trait StorePathInfos {
    type Error: Error + Send;

    fn content_address(
        &mut self,
    ) -> Result<Option<ContentAddress>, Self::Error>;

    fn deriver(&mut self) -> Result<Option<StoreBasename>, Self::Error>;

    fn nar_hash(&mut self) -> Result<Nix32Digest<32>, Self::Error>;

    fn nar_size(&mut self) -> Result<NonZeroU64, Self::Error>;

    fn references(
        &mut self,
    ) -> Result<impl IntoIterator<Item = StoreBasename> + use<Self>, Self::Error>;

    fn signatures(
        &mut self,
    ) -> Result<impl IntoIterator<Item = Signature> + use<Self>, Self::Error>;
}

pub trait Spawner {
    type JoinHandle<Fut: Future>: JoinHandle<Fut::Output>;

    fn spawn<Fut>(&self, future: Fut) -> Self::JoinHandle<Fut>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static;
}

pub trait JoinHandle<Output>: Future<Output = Output> {
    fn detach(self);
}
