//! The wire protocol spoken on the Unix domain socket between the daemon and
//! the `push`/`stop` subcommands.
//!
//! Store paths are separated by newlines, and a null byte signals the daemon to
//! stop and shut down. A line starting with a `0x01` byte holds a
//! whitespace-separated list of substituters.
//!
//! ```text
//! push: \x01https://cache.nixos.org/\n/nix/store/abc-foo\n/nix/store/def-bar\n
//! stop: \0
//! ```

use core::ops::Range;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use core::{fmt, str};
use std::io;

use futures::{AsyncRead, AsyncWrite};
use nix_types::{NixStoreLiteral, StorePath};
use smol_str::SmolStr;

/// The lines separator in the wire format.
const LINE_SEPARATOR: u8 = b'\n';

/// The maximum length of a line, separator excluded.
const MAX_LINE_LEN: usize = 64 * 1024;

/// The sentinal byte which signals the daemon to stop pending uploads and shut
/// down.
const STOP_SENTINEL: u8 = b'\0';

/// The byte starting a line of substituters.
const SUBSTITUTERS_PREFIX: u8 = 0x01;

pin_project_lite::pin_project! {
    pub(crate) struct Sender<Writer> {
        buf: Vec<u8>,
        // The number of bytes in [`buf`](Sender::buf) we've already written to
        // the underlying writer.
        num_written: usize,
        #[pin]
        writer: Writer,
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct Receiver<Reader> {
        is_done: bool,
        // The buffer bytes are read into, which grows to fit the longest line.
        buf: Vec<u8>,
        // The range of bytes in [`buf`](Receiver::buf) we still have to handle.
        unhandled_range: Range<usize>,
        #[pin]
        reader: Reader,
    }
}

#[derive(Debug)]
pub(crate) enum Message {
    PushStorePath(StorePath<StoreDir>),
    StopDaemon,
    /// A whitespace-separated list of substituters, without newlines.
    Substituters(String),
}

#[derive(Clone)]
pub enum StoreDir {
    /// The `/nix/store` directory.
    NixStore,
    /// Any other store directory, guaranteed to not contain the `NUL` byte.
    Other(SmolStr),
}

#[derive(Debug)]
pub(crate) enum ReceiveError {
    Io(io::Error),
    InvalidStorePath(<StorePath<StoreDir> as str::FromStr>::Err),
    LineTooLong,
}

impl<Writer> Sender<Writer> {
    pub(crate) fn new(writer: Writer) -> Self {
        Self { writer, buf: Vec::new(), num_written: 0 }
    }
}

impl<Writer: AsyncWrite> Sender<Writer> {
    /// Writes all buffered bytes to the underlying writer and clears the
    /// buffer.
    fn drain_buf(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let mut this = self.project();

        while *this.num_written < this.buf.len() {
            let num_written = ready!(
                this.writer
                    .as_mut()
                    .poll_write(ctx, &this.buf[*this.num_written..])
            )?;

            if num_written == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned 0",
                )));
            }

            *this.num_written += num_written;
        }

        this.buf.clear();
        *this.num_written = 0;

        Poll::Ready(Ok(()))
    }
}

impl<Writer: AsyncWrite> futures::Sink<Message> for Sender<Writer> {
    type Error = io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        // Messages are buffered in memory and written to the underlying writer
        // on flush/close, so we're always ready
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: Message,
    ) -> Result<(), Self::Error> {
        use std::io::Write;

        let buf = self.project().buf;

        match message {
            Message::PushStorePath(store_path) => {
                write!(buf, "{store_path}")
                    .expect("writing to a Vec never fails");
                buf.push(LINE_SEPARATOR);
            },
            Message::StopDaemon => {
                buf.push(STOP_SENTINEL);
            },
            Message::Substituters(substituters) => {
                buf.push(SUBSTITUTERS_PREFIX);
                buf.extend_from_slice(substituters.as_bytes());
                buf.push(LINE_SEPARATOR);
            },
        }

        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().drain_buf(ctx))?;
        self.project().writer.poll_flush(ctx)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().drain_buf(ctx))?;
        self.project().writer.poll_close(ctx)
    }
}

impl<Reader> Receiver<Reader> {
    pub(crate) fn new(reader: Reader) -> Self {
        Self {
            is_done: false,
            reader,
            buf: vec![0; 1024],
            unhandled_range: 0..0,
        }
    }
}

impl<Reader: AsyncRead> futures::Stream for Receiver<Reader> {
    type Item = Result<Message, ReceiveError>;

    fn poll_next(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.is_done {
            return Poll::Ready(None);
        }

        let mut this = self.project();

        let res = loop {
            let bytes = &this.buf[this.unhandled_range.clone()];

            if let Some(&STOP_SENTINEL) = bytes.first() {
                break Ok(Message::StopDaemon);
            }

            let Some(separator_offset) = memchr::memchr(LINE_SEPARATOR, bytes)
            else {
                let bytes_len = bytes.len();
                if bytes_len > MAX_LINE_LEN {
                    break Err(ReceiveError::LineTooLong);
                }
                this.buf.copy_within(this.unhandled_range.clone(), 0);
                *this.unhandled_range = 0..bytes_len;
                if bytes_len == this.buf.len() {
                    this.buf.resize(bytes_len * 2, 0);
                }
                let read_buf = &mut this.buf[this.unhandled_range.end..];
                match ready!(this.reader.as_mut().poll_read(ctx, read_buf)) {
                    Ok(0) => return Poll::Ready(None),
                    Ok(num_read) => this.unhandled_range.end += num_read,
                    Err(err) => break Err(ReceiveError::Io(err)),
                }
                continue;
            };

            let line = &bytes[..separator_offset];

            this.unhandled_range.start += separator_offset + 1;

            let line = match str::from_utf8(line) {
                Ok(s) => s,
                Err(err) => {
                    break Err(ReceiveError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        err,
                    )));
                },
            };

            if let Some(substituters) =
                line.strip_prefix(char::from(SUBSTITUTERS_PREFIX))
            {
                break Ok(Message::Substituters(substituters.to_owned()));
            }

            break match line.parse() {
                Ok(store_path) => Ok(Message::PushStorePath(store_path)),
                Err(err) => Err(ReceiveError::InvalidStorePath(err)),
            };
        };

        *this.is_done = matches!(res, Ok(Message::StopDaemon) | Err(_));

        Poll::Ready(Some(res))
    }
}

impl fmt::Display for StoreDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreDir::NixStore => NixStoreLiteral.fmt(f),
            StoreDir::Other(smol_str) => smol_str.fmt(f),
        }
    }
}

impl str::FromStr for StoreDir {
    type Err = core::ffi::FromBytesWithNulError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == NixStoreLiteral::STR {
            Ok(Self::NixStore)
        } else if let Some(position) = memchr::memchr(0, s.as_bytes()) {
            Err(core::ffi::FromBytesWithNulError::InteriorNul { position })
        } else {
            Ok(Self::Other(s.into()))
        }
    }
}

impl fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::InvalidStorePath(error) => {
                write!(f, "invalid store path: {error}")
            },
            Self::LineTooLong => {
                write!(f, "line longer than {MAX_LINE_LEN} bytes")
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use core::pin::pin;

    use futures::io::Cursor;
    use futures::{SinkExt, StreamExt};

    use super::*;

    #[test]
    fn send_single_store_path() {
        let bytes = send_messages(vec![Message::PushStorePath(path(
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello",
        ))]);

        assert_eq!(
            bytes,
            b"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello\n",
        );
    }

    #[test]
    fn send_stop() {
        assert_eq!(send_messages(vec![Message::StopDaemon]), b"\0");
    }

    #[test]
    fn send_paths_then_stop() {
        let bytes = send_messages(vec![
            Message::PushStorePath(path(
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo",
            )),
            Message::PushStorePath(path(
                "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bar",
            )),
            Message::StopDaemon,
        ]);

        assert_eq!(
            bytes,
            b"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo\n\
              /nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bar\n\
              \0",
        );
    }

    #[test]
    fn receive_single_store_path() {
        let msgs = parse_messages(
            b"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello\n",
        );
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            Ok(Message::PushStorePath(p)) => assert_eq!(
                p.to_string(),
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello",
            ),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn receive_multiple_store_paths() {
        let msgs = parse_messages(
            b"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo\n\
              /nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bar\n",
        );
        assert_eq!(msgs.len(), 2);
        assert!(matches!(&msgs[0], Ok(Message::PushStorePath(_))));
        assert!(matches!(&msgs[1], Ok(Message::PushStorePath(_))));
    }

    #[test]
    fn receive_stop() {
        let msgs = parse_messages(b"\0");
        assert_eq!(msgs.len(), 1);
        assert!(matches!(msgs[0], Ok(Message::StopDaemon)));
    }

    #[test]
    fn receive_paths_then_stop() {
        let msgs = parse_messages(
            b"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo\n\0",
        );
        assert_eq!(msgs.len(), 2);
        assert!(matches!(msgs[0], Ok(Message::PushStorePath(_))));
        assert!(matches!(msgs[1], Ok(Message::StopDaemon)));
    }

    #[test]
    fn send_substituters_then_path() {
        let bytes = send_messages(vec![
            Message::Substituters(
                "https://a.example/ https://b.example".into(),
            ),
            Message::PushStorePath(path(
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo",
            )),
        ]);

        assert_eq!(
            bytes,
            b"\x01https://a.example/ https://b.example\n\
              /nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo\n",
        );
    }

    #[test]
    fn receive_substituters_then_path() {
        let msgs = parse_messages(
            b"\x01https://a.example/ https://b.example\n\
              /nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo\n",
        );
        assert_eq!(msgs.len(), 2);
        match &msgs[0] {
            Ok(Message::Substituters(substituters)) => {
                assert_eq!(substituters, "https://a.example/ https://b.example")
            },
            other => panic!("unexpected: {other:?}"),
        }
        assert!(matches!(&msgs[1], Ok(Message::PushStorePath(_))));
    }

    #[test]
    fn receive_empty_substituters() {
        let msgs = parse_messages(b"\x01\n");
        assert_eq!(msgs.len(), 1);
        assert!(
            matches!(&msgs[0], Ok(Message::Substituters(s)) if s.is_empty())
        );
    }

    #[test]
    fn receive_line_longer_than_initial_buffer() {
        let substituters = "https://a.example/ ".repeat(200);
        let mut input = vec![SUBSTITUTERS_PREFIX];
        input.extend_from_slice(substituters.as_bytes());
        input.extend_from_slice(
            b"\n/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo\n",
        );
        let msgs = parse_messages(&input);
        assert_eq!(msgs.len(), 2);
        assert!(
            matches!(&msgs[0], Ok(Message::Substituters(s)) if *s == substituters)
        );
        assert!(matches!(&msgs[1], Ok(Message::PushStorePath(_))));
    }

    #[test]
    fn receive_line_too_long() {
        let msgs = parse_messages(&vec![b'a'; MAX_LINE_LEN + 2]);
        assert_eq!(msgs.len(), 1);
        assert!(matches!(msgs[0], Err(ReceiveError::LineTooLong)));
    }

    #[test]
    fn receive_empty_input() {
        assert!(parse_messages(b"").is_empty());
    }

    #[test]
    fn receive_invalid_store_path() {
        let msgs = parse_messages(b"not-a-store-path\n");
        assert_eq!(msgs.len(), 1);
        assert!(matches!(msgs[0], Err(ReceiveError::InvalidStorePath(_))));
    }

    fn parse_messages(input: &[u8]) -> Vec<Result<Message, ReceiveError>> {
        futures::executor::block_on(async {
            let receiver = Receiver::new(Cursor::new(input.to_vec()));
            futures::pin_mut!(receiver);
            let mut results = Vec::new();
            while let Some(item) = receiver.next().await {
                results.push(item);
            }
            results
        })
    }

    fn send_messages(messages: Vec<Message>) -> Vec<u8> {
        futures::executor::block_on(async {
            let buf = Cursor::new(Vec::new());
            let mut sender = pin!(Sender::new(buf));
            for msg in messages {
                sender.as_mut().feed(msg).await.unwrap();
            }
            sender.as_mut().close().await.unwrap();
            sender.project().writer.get_mut().clone().into_inner()
        })
    }

    fn path(s: &str) -> StorePath<StoreDir> {
        s.parse().unwrap()
    }
}
