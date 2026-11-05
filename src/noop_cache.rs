use core::convert::Infallible;
use core::num::NonZeroU64;

use futures::AsyncRead;
use nix_types::{NarFileName, NarInfo, NarInfoFileName, StoreHash};

use crate::context::Cache;
use crate::protocol::StoreDir;
use crate::real_cache::RealCache;
use crate::token::SharedToken;

#[derive(Default, Clone, Copy)]
pub(crate) struct NoopCache {}

impl Cache for NoopCache {
    type Error = Infallible;

    // Mirrors the real cache, so that both upload paths are exercised.
    const MAX_SINGLE_PART_NAR_SIZE: u64 =
        <RealCache as Cache>::MAX_SINGLE_PART_NAR_SIZE;

    async fn missing_narinfos(
        &self,
        _: &SharedToken,
        store_hashes: &[StoreHash],
    ) -> Result<Vec<StoreHash>, Self::Error> {
        Ok(store_hashes.to_vec())
    }

    async fn upload_nar_multipart(
        &self,
        _: &SharedToken,
        _: NarFileName,
        _: NonZeroU64,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<u64, Self::Error> {
        let mut sink = futures::io::sink();
        Ok(futures::io::copy(nar_bytes, &mut sink).await.expect("never fails"))
    }

    async fn upload_nar_single_part(
        &self,
        _: &SharedToken,
        _: NarFileName,
        nar_bytes: Vec<u8>,
    ) -> Result<u64, Self::Error> {
        Ok(nar_bytes.len() as u64)
    }

    async fn upload_narinfo(
        &self,
        _: &SharedToken,
        _: NarInfoFileName,
        narinfo: NarInfo<(), StoreDir>,
    ) -> Result<u64, Self::Error> {
        Ok(narinfo.with_url("").to_string().len() as u64)
    }
}
