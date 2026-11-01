use futures::AsyncWrite;

use crate::nix_store::NixStore;
use crate::real_stop_progress_reporter::RealStopProgressReporter;
use crate::real_token_fetcher::{FetchTokenError, RealTokenFetcher};
use crate::tokio::TokioSpawner;
use crate::{context, protocol};

pub(crate) struct RealContext {
    http_client: reqwest::Client,
    nix_store: NixStore,
    spawner: TokioSpawner,
    token_fetcher: RealTokenFetcher,
}

impl RealContext {
    pub(crate) fn new(
        nix_store: NixStore,
        spawner: TokioSpawner,
        token_fetcher: RealTokenFetcher,
    ) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            nix_store,
            spawner,
            token_fetcher,
        }
    }
}
impl context::Context for RealContext {
    #[cfg(not(feature = "sub60-cache"))]
    type Cache = crate::any_cache::AnyCache;
    #[cfg(feature = "sub60-cache")]
    type Cache = crate::real_cache::RealCache;
    type FetchTokenError = FetchTokenError;
    type HttpClient = reqwest::Client;
    type StopProgressReporter<W: AsyncWrite + Unpin> =
        RealStopProgressReporter<W>;
    type Nix = NixStore;
    type Spawner = TokioSpawner;

    fn handle_rx_error(&mut self, rx_error: protocol::ReceiveError) {
        eprintln!("{rx_error}")
    }

    fn http_client(&self) -> &Self::HttpClient {
        &self.http_client
    }

    fn new_stop_progress_reporter<W: AsyncWrite + Unpin>(
        &mut self,
        writer: W,
    ) -> Self::StopProgressReporter<W> {
        RealStopProgressReporter::new(writer)
    }

    fn nix(&self) -> &Self::Nix {
        &self.nix_store
    }

    fn spawner(&self) -> &Self::Spawner {
        &self.spawner
    }

    fn token_fetcher(&self) -> RealTokenFetcher {
        self.token_fetcher.clone()
    }
}

impl context::HttpClient for reqwest::Client {
    type Error = reqwest::Error;

    async fn get(
        &self,
        url: url::Url,
    ) -> Result<http::StatusCode, Self::Error> {
        self.get(url).send().await.map(|response| response.status())
    }
}
