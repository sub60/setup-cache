//! Serializing file system objects into NARs, the archive format Nix hashes and
//! copies store paths as.

use std::collections::{BTreeMap, btree_map};
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use async_compat::Compat;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::fs;

/// Writes the NAR of the file system object at `path`.
pub(crate) async fn dump_path(
    path: &Path,
    use_case_hack: bool,
    writer: &mut (impl AsyncWrite + Unpin),
) -> io::Result<()> {
    write_str(writer, b"nix-archive-1").await?;
    dump_node(path, use_case_hack, writer).await
}

async fn dump_node(
    path: &Path,
    use_case_hack: bool,
    writer: &mut (impl AsyncWrite + Unpin),
) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path).await?;
    let file_type = metadata.file_type();

    write_str(writer, b"(").await?;
    write_str(writer, b"type").await?;

    if file_type.is_dir() {
        write_str(writer, b"directory").await?;
        let mut child_path = path.to_owned();
        for (nar_name, file_name) in
            read_dir_entries(path, use_case_hack).await?
        {
            write_str(writer, b"entry").await?;
            write_str(writer, b"(").await?;
            write_str(writer, b"name").await?;
            write_str(writer, nar_name.as_bytes()).await?;
            write_str(writer, b"node").await?;
            child_path.push(&file_name);
            Box::pin(dump_node(&child_path, use_case_hack, writer)).await?;
            child_path.pop();
            write_str(writer, b")").await?;
        }
    } else if file_type.is_symlink() {
        let target = fs::read_link(path).await?;
        write_str(writer, b"symlink").await?;
        write_str(writer, b"target").await?;
        write_str(writer, target.as_os_str().as_bytes()).await?;
    } else if file_type.is_file() {
        write_str(writer, b"regular").await?;
        if metadata.permissions().mode() & 0o100 != 0 {
            write_str(writer, b"executable").await?;
            write_str(writer, b"").await?;
        }
        write_str(writer, b"contents").await?;
        let file = Compat::new(fs::File::open(path).await?);
        write_contents(writer, metadata.len(), file).await?;
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported file type at {}", path.display()),
        ));
    }

    write_str(writer, b")").await
}

/// Writes the `len` bytes of a file's contents, which are framed like a string.
async fn write_contents(
    writer: &mut (impl AsyncWrite + Unpin),
    len: u64,
    contents: impl AsyncRead + Unpin,
) -> io::Result<()> {
    writer.write_all(&len.to_le_bytes()).await?;

    if futures::io::copy(contents.take(len), writer).await? != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "file got shorter while it was being read",
        ));
    }

    write_padding(writer, len).await
}

/// Writes a string as its length, its bytes, and zeros up to a multiple of 8
/// bytes.
async fn write_str(
    writer: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
) -> io::Result<()> {
    let len = bytes.len() as u64;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(bytes).await?;
    write_padding(writer, len).await
}

async fn write_padding(
    writer: &mut (impl AsyncWrite + Unpin),
    len: u64,
) -> io::Result<()> {
    let padding = (8 - len % 8) % 8;
    writer.write_all(&[0; 8][..padding as usize]).await
}

/// Returns a map from each child's logical NAR name to its file name, sorted by
/// logical name.
///
/// The file name is the directory entry that exists on disk and is used to open
/// the child, while the logical name is the name written to the NAR, which
/// contributes to the NAR's canonical contents and hash. These are normally
/// identical.
///
/// When `use-case-hack` is enabled, Nix can store names that differ only in
/// casing onto a case-insensitive filesystem – such as the default macOS
/// filesystem – by appending `~nix~case~hack~<number>` to colliding file names.
/// When dumping the path again, that suffix must be removed to recover the
/// original logical NAR name while the suffixed file name is retained for
/// filesystem access.
async fn read_dir_entries(
    dir_path: &Path,
    use_case_hack: bool,
) -> io::Result<BTreeMap<OsString, OsString>> {
    let mut read_dir = fs::read_dir(dir_path).await?;
    let mut entries = BTreeMap::new();
    while let Some(entry) = read_dir.next_entry().await? {
        let file_name = entry.file_name();
        let logical_name = logical_name(&file_name, use_case_hack);
        match entries.entry(logical_name) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(file_name);
            },
            btree_map::Entry::Occupied(entry) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "file name collision between {:?} and {file_name:?} \
                         after decoding Nix's case hack",
                        entry.get(),
                    ),
                ));
            },
        }
    }
    Ok(entries)
}

fn logical_name(file_name: &OsStr, use_case_hack: bool) -> OsString {
    const CASE_HACK_SUFFIX: &[u8] = b"~nix~case~hack~";

    let bytes = file_name.as_bytes();

    if use_case_hack
        && let Some(suffix_start) =
            memchr::memmem::find(bytes, CASE_HACK_SUFFIX)
    {
        OsString::from_vec(bytes[..suffix_start].to_vec())
    } else {
        file_name.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::process::Command;

    use super::*;

    fn dump(path: &Path, use_case_hack: bool) -> io::Result<Vec<u8>> {
        let mut nar = Vec::new();
        ::tokio::runtime::Runtime::new().unwrap().block_on(dump_path(
            path,
            use_case_hack,
            &mut nar,
        ))?;
        Ok(nar)
    }

    #[test]
    fn matches_nix_store_dump() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");

        fs::create_dir_all(root.join("sub/empty-dir")).unwrap();
        fs::write(root.join("empty"), "").unwrap();
        fs::write(root.join("eight"), "12345678").unwrap();
        fs::write(root.join("hello"), "hello\n").unwrap();
        fs::write(root.join("sub/nested"), "nested").unwrap();
        fs::write(root.join("ünïcödé"), "ü").unwrap();
        // Entries are sorted by their bytes, so `B` comes before `a`.
        fs::write(root.join("a"), "a").unwrap();
        fs::write(root.join("B"), "B").unwrap();
        fs::write(root.join("script"), "#!/bin/sh\n").unwrap();
        fs::set_permissions(
            root.join("script"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        symlink("hello", root.join("link")).unwrap();
        symlink("does/not/exist", root.join("dangling")).unwrap();

        let output = Command::new("nix-store")
            .args(["--option", "use-case-hack", "false", "--dump"])
            .arg(&root)
            .output()
            .expect("`nix-store` is installed");

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(dump(&root, false).unwrap() == output.stdout);
    }

    #[test]
    fn writes_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("link");
        symlink("target", &link).unwrap();

        let expected: &[u8] = b"\
            \x0d\0\0\0\0\0\0\0nix-archive-1\0\0\0\
            \x01\0\0\0\0\0\0\0(\0\0\0\0\0\0\0\
            \x04\0\0\0\0\0\0\0type\0\0\0\0\
            \x07\0\0\0\0\0\0\0symlink\0\
            \x06\0\0\0\0\0\0\0target\0\0\
            \x06\0\0\0\0\0\0\0target\0\0\
            \x01\0\0\0\0\0\0\0)\0\0\0\0\0\0\0";

        assert_eq!(dump(&link, false).unwrap(), expected);
    }

    #[test]
    fn rejects_files_that_got_shorter() {
        let runtime = ::tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(write_contents(&mut Vec::new(), 10, &b"short"[..]))
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn decodes_case_hack_suffix() {
        let file_name = OsStr::from_bytes(b"pod~nix~case~hack~1");

        assert_eq!(logical_name(file_name, true).as_bytes(), b"pod");
        assert_eq!(
            logical_name(file_name, false).as_bytes(),
            file_name.as_bytes(),
        );
    }

    #[test]
    fn rejects_case_hack_name_collisions() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("pod"), []).unwrap();
        fs::write(dir.path().join("pod~nix~case~hack~1"), []).unwrap();

        let runtime = ::tokio::runtime::Runtime::new().unwrap();
        let error =
            runtime.block_on(read_dir_entries(dir.path(), true)).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
