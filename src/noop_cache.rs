use core::convert::Infallible;
use core::num::NonZeroU64;

use futures::AsyncRead;
use nix_types::{NarFileName, NarInfo, NarInfoFileName};

use crate::context::Cache;
use crate::protocol::StoreDir;

#[derive(Default, Clone, Copy)]
pub(crate) struct NoopCache {}

impl Cache for NoopCache {
    type Error = Infallible;

    async fn has_narinfo(
        &self,
        _: &NarInfoFileName,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn upload_nar(
        &self,
        _: NarFileName,
        _: NonZeroU64,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<u64, Self::Error> {
        let mut sink = futures::io::sink();
        Ok(futures::io::copy(nar_bytes, &mut sink).await.expect("never fails"))
    }

    async fn upload_narinfo(
        &self,
        _: NarInfoFileName,
        narinfo: NarInfo<(), StoreDir>,
    ) -> Result<u64, Self::Error> {
        Ok(narinfo.with_url("").to_string().len() as u64)
    }
}
