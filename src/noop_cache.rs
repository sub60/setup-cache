use core::convert::Infallible;
use core::num::NonZeroU64;
use core::pin::pin;

use futures::{AsyncRead, AsyncReadExt};
use nix_types::{
    NarFileName,
    NarInfo,
    NarInfoFileName,
    Nix32Digest,
    StoreHash,
};
use sha2::{Digest, Sha256};
use sub60_cache_api::endpoints::nar_multiparts_complete;

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
        _: NonZeroU64,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<nar_multiparts_complete::Response, Self::Error> {
        let mut nar_bytes = pin!(nar_bytes);
        let mut sha256 = Sha256::new();
        let mut buf = vec![0; 64 * 1024];

        loop {
            match nar_bytes.read(&mut buf).await.expect("never fails") {
                0 => break,
                num_read => sha256.update(&buf[..num_read]),
            }
        }

        Ok(nar_multiparts_complete::Response::Created(
            nar_multiparts_complete::ResponseBody {
                file_hash: Nix32Digest::new(&sha256.finalize().into()),
            },
        ))
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
