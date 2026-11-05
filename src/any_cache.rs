use core::num::NonZeroU64;

use futures::AsyncRead;
use nix_types::{NarFileName, NarInfo, NarInfoFileName, StoreHash};

use crate::context::Cache;
use crate::noop_cache::NoopCache;
use crate::protocol::StoreDir;
use crate::real_cache::{RealCache, RealCacheConnectError};
use crate::run::RunArgs;
use crate::token::Token;

#[derive(Clone)]
pub(crate) enum AnyCache {
    Noop(NoopCache),
    Real(RealCache),
}

#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub(crate) enum AnyCacheKind {
    Noop,
    Real,
}

#[derive(Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum AnyCacheError {
    #[display("no-op cache error")]
    Noop(#[source] <NoopCache as Cache>::Error),
    #[display("real cache error")]
    Real(#[source] <RealCache as Cache>::Error),
}

impl AnyCache {
    pub(crate) async fn new(
        args: &RunArgs,
    ) -> Result<Self, RealCacheConnectError> {
        match args.cache {
            AnyCacheKind::Noop => Ok(NoopCache::default().into()),
            AnyCacheKind::Real => {
                RealCache::connect(args).await.map(Into::into)
            },
        }
    }
}

impl Cache for AnyCache {
    type Error = AnyCacheError;

    async fn missing_narinfos(
        &self,
        token: &Token,
        store_hashes: &[StoreHash],
    ) -> Result<Vec<StoreHash>, Self::Error> {
        match self {
            Self::Noop(cache) => cache
                .missing_narinfos(token, store_hashes)
                .await
                .map_err(Into::into),
            Self::Real(cache) => cache
                .missing_narinfos(token, store_hashes)
                .await
                .map_err(Into::into),
        }
    }

    async fn upload_nar(
        &self,
        token: &Token,
        nar_file_name: NarFileName,
        nar_size: NonZeroU64,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<u64, Self::Error> {
        match self {
            Self::Noop(cache) => cache
                .upload_nar(token, nar_file_name, nar_size, nar_bytes)
                .await
                .map_err(Into::into),
            Self::Real(cache) => cache
                .upload_nar(token, nar_file_name, nar_size, nar_bytes)
                .await
                .map_err(Into::into),
        }
    }

    async fn upload_narinfo(
        &self,
        token: &Token,
        narinfo_filename: NarInfoFileName,
        narinfo: NarInfo<(), StoreDir>,
    ) -> Result<u64, Self::Error> {
        match self {
            Self::Noop(cache) => cache
                .upload_narinfo(token, narinfo_filename, narinfo)
                .await
                .map_err(Into::into),
            Self::Real(cache) => cache
                .upload_narinfo(token, narinfo_filename, narinfo)
                .await
                .map_err(Into::into),
        }
    }
}

impl From<NoopCache> for AnyCache {
    fn from(cache: NoopCache) -> Self {
        Self::Noop(cache)
    }
}

impl From<RealCache> for AnyCache {
    fn from(cache: RealCache) -> Self {
        Self::Real(cache)
    }
}

impl From<<NoopCache as Cache>::Error> for AnyCacheError {
    fn from(error: <NoopCache as Cache>::Error) -> Self {
        Self::Noop(error)
    }
}

impl From<<RealCache as Cache>::Error> for AnyCacheError {
    fn from(error: <RealCache as Cache>::Error) -> Self {
        Self::Real(error)
    }
}
