use core::ffi::CStr;
use core::num::NonZeroU64;
use core::ops::ControlFlow;
use std::ffi::OsString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use nix_compat::nar::writer::r#async as nar_writer;
use nix_types::{
    ContentAddress,
    Nix32Digest,
    Signature,
    StoreBasename,
    StorePath,
};
use nixb::store::GetFsClosureOpts;
use smallvec::SmallVec;
use tokio::fs;
use tokio::io::BufReader;

use crate::context;
use crate::protocol::StoreDir;

#[derive(Clone)]
pub(crate) struct NixStore {
    store: nixb::store::Store,
}

trait StorePathExt {
    fn basename(&self) -> nixb::Result<StoreBasename>;
}

impl NixStore {
    pub(crate) fn open() -> nixb::Result<Self> {
        let mut ctx = nixb::contexts::c_context::CContext::create();
        let init = nixb::store::init::<true>(&mut ctx)?;
        let store = nixb::store::Store::open(init, c"", [], ctx)?;
        Ok(Self { store })
    }

    fn parse_path(
        &mut self,
        store_path: &StorePath<StoreDir>,
    ) -> nixb::Result<nixb::store::StorePath> {
        let mut bytes = store_path.to_string().into_bytes();
        bytes.push(0);
        // SAFETY: we just pushed a trailing NUL byte, and both the StoreDir and
        // the store basename are guaranteed to not contain any interior NULs.
        let cstr = unsafe { CStr::from_bytes_with_nul_unchecked(&bytes) };
        self.store.parse_path(cstr)
    }
}

impl context::Nix for NixStore {
    type PathInfos<'a> = nixb::store::PathInfo;

    type PathInfosError = nixb::Error;
    type ReadNarError = io::Error;
    type StoreClosureError = nixb::Error;

    async fn get_path_infos<'a>(
        &mut self,
        store_path: &'a StorePath<StoreDir>,
    ) -> Result<Self::PathInfos<'a>, Self::PathInfosError> {
        let path = self.parse_path(store_path)?;
        self.store.query_path_info(&path)
    }

    async fn read_nar(
        &mut self,
        store_path: &StorePath<StoreDir>,
        buf: &mut (impl futures::AsyncWrite + Unpin + Send),
    ) -> Result<(), Self::ReadNarError> {
        let store_path = store_path.to_string();
        let mut writer = async_compat::Compat::new(buf);
        let root = nar_writer::open(&mut writer).await?;
        write_nar(root, Path::new(&store_path)).await?;
        Ok(())
    }

    async fn store_closure(
        &mut self,
        store_path: &StorePath<StoreDir>,
    ) -> Result<Vec<StorePath<StoreDir>>, Self::StoreClosureError> {
        let path = self.parse_path(store_path)?;

        let mut res = Ok(vec![]);

        let push_path = |path: &nixb::store::StorePath| {
            let Ok(paths) = &mut res else { return };
            let basename = match path.basename() {
                Ok(basename) => basename,
                Err(err) => {
                    res = Err(err);
                    return;
                },
            };
            let store_dir = store_path.store_dir().clone();
            paths.push(StorePath::new(basename).with_store_dir(store_dir));
        };

        self.store.get_fs_closure(
            &path,
            push_path,
            GetFsClosureOpts::default(),
        )?;

        res
    }
}

impl context::StorePathInfos for nixb::store::PathInfo {
    type Error = nixb::Error;

    fn content_address(
        &mut self,
    ) -> Result<Option<ContentAddress>, Self::Error> {
        self.with_ca(|ca| {
            ca.parse().map_err(|err| {
                nixb::Error::from_message(format_args!(
                    "couldn't parse {ca:?} into a CA: {err}"
                ))
            })
        })?
        .transpose()
    }

    fn deriver(&mut self) -> Result<Option<StoreBasename>, Self::Error> {
        let Some(store_path) = self.get_deriver()? else { return Ok(None) };
        store_path.basename().map(Some)
    }

    fn nar_hash(&mut self) -> Result<Nix32Digest<32>, Self::Error> {
        self.with_nar_hash(|nar_hash| {
            let Some(nar_hash) = nar_hash.strip_prefix("sha256:") else {
                return Err(nixb::Error::from_message(format_args!(
                    "NAR hash is not SHA-256: {nar_hash:?}"
                )));
            };

            nar_hash.parse().map_err(|err| {
                nixb::Error::from_message(format_args!(
                    "couldn't parse NAR hash {nar_hash:?}: {err}"
                ))
            })
        })?
    }

    fn nar_size(&mut self) -> Result<NonZeroU64, Self::Error> {
        self.get_nar_size()?.ok_or_else(|| {
            nixb::Error::from_message("NAR size is missing or zero")
        })
    }

    fn references(
        &mut self,
    ) -> Result<SmallVec<[StoreBasename; 2]>, Self::Error> {
        let mut references = SmallVec::new();

        let control_flow =
            self.with_references(|store_path| match store_path.basename() {
                Ok(basename) => {
                    references.push(basename);
                    ControlFlow::Continue(())
                },
                Err(err) => ControlFlow::Break(err),
            })?;

        match control_flow {
            ControlFlow::Continue(()) => Ok(references),
            ControlFlow::Break(err) => Err(err),
        }
    }

    fn signatures(&mut self) -> Result<SmallVec<[Signature; 2]>, Self::Error> {
        let mut signatures = SmallVec::new();

        let control_flow = self.with_sigs(|sig_bytes| {
            match signature_from_bytes(sig_bytes) {
                Ok(signature) => {
                    signatures.push(signature);
                    ControlFlow::Continue(())
                },
                Err(err) => ControlFlow::Break(err),
            }
        })?;

        match control_flow {
            ControlFlow::Continue(()) => Ok(signatures),
            ControlFlow::Break(err) => Err(err),
        }
    }
}

impl StorePathExt for nixb::store::StorePath {
    fn basename(&self) -> nixb::Result<StoreBasename> {
        let hash = self.hash()?;
        let name = self.with_name(|name| {
            name.parse().map_err(|err| {
                nixb::Error::from_message(format_args!(
                    "couldn't parse {name:?} into a store name: {err}"
                ))
            })
        })?;
        Ok(StoreBasename { hash: Nix32Digest::new(&hash), name })
    }
}

fn signature_from_bytes(bytes: &[u8]) -> nixb::Result<Signature> {
    let sig_str = str::from_utf8(bytes).map_err(|_err| {
        nixb::Error::from_message(format_args!(
            "signature is not valid UTF-8: {:?}",
            String::from_utf8_lossy(bytes)
        ))
    })?;

    sig_str.parse().map_err(|err| {
        nixb::Error::from_message(format_args!(
            "couldn't parse {sig_str:?} into signature: {err}",
        ))
    })
}

async fn write_nar(
    node: nar_writer::Node<'_, '_>,
    node_path: &Path,
) -> io::Result<()> {
    let metadata = fs::symlink_metadata(node_path).await?;
    let file_type = metadata.file_type();

    if file_type.is_dir() {
        let mut directory = node.directory().await?;
        let mut child_path = node_path.to_owned();
        for child_name in read_dir_entries(node_path).await? {
            let child = directory.entry(child_name.as_bytes()).await?;
            child_path.push(&child_name);
            Box::pin(write_nar(child, &child_path)).await?;
            child_path.pop();
        }
        return directory.close().await;
    }

    if file_type.is_symlink() {
        let target = fs::read_link(node_path).await?;
        return node.symlink(target.as_os_str().as_bytes()).await;
    }

    if file_type.is_file() {
        let executable = metadata.permissions().mode() & 0o100 != 0;
        let file = fs::File::open(node_path).await?;
        let mut reader = BufReader::new(file);
        return node.file(executable, metadata.len(), &mut reader).await;
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unsupported file type at {}", node_path.display()),
    ))
}

/// Returns the file names of the children of the directory at the given path,
/// sorted lexicographically.
async fn read_dir_entries(dir_path: &Path) -> io::Result<Vec<OsString>> {
    let mut read_dir = fs::read_dir(dir_path).await?;
    let mut file_names = vec![];
    while let Some(entry) = read_dir.next_entry().await? {
        file_names.push(entry.file_name());
    }
    file_names.sort();
    Ok(file_names)
}
