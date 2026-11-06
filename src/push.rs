//! TODO: docs.

use core::fmt;
use core::pin::pin;
use std::path::PathBuf;
use std::{env, io, process};

use futures::SinkExt;
use nix_types::StorePath;
use tokio::net::UnixStream;

use crate::protocol::{self, StoreDir};
use crate::tokio::Tokio;

#[derive(Debug, clap::Args)]
pub struct PushArgs {
    #[arg(long)]
    socket: PathBuf,
    #[arg(required = true)]
    store_paths: Vec<StorePath<StoreDir>>,
}

enum PushError {
    ConnectToSocket(io::Error),
    WriteMessage(io::Error),
}

pub(crate) fn push(args: PushArgs) {
    let push_result: Result<(), PushError> =
        Tokio::new().block_on(async move {
            let socket = UnixStream::connect(&args.socket)
                .await
                .map_err(PushError::ConnectToSocket)?;

            let mut sender =
                pin!(protocol::Sender::new(async_compat::Compat::new(socket)));

            // Nix passes its settings to the post-build hook in $NIX_CONFIG
            // since 2.4. With older versions the daemon gets no substituters.
            if let Some(substituters) =
                env::var("NIX_CONFIG").ok().as_deref().and_then(|config| {
                    config
                        .lines()
                        .find_map(|line| line.strip_prefix("substituters = "))
                })
            {
                sender
                    .feed(protocol::Message::Substituters(substituters.into()))
                    .await
                    .map_err(PushError::WriteMessage)?;
            }

            for store_path in args.store_paths {
                sender
                    .feed(protocol::Message::PushStorePath(store_path))
                    .await
                    .map_err(PushError::WriteMessage)?;
            }

            sender.close().await.map_err(PushError::WriteMessage)
        });

    if let Err(push_error) = push_result {
        eprintln!("{push_error}");
        process::exit(1);
    }
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectToSocket(error) => {
                write!(f, "Couldn't connect to Unix domain socket: {error}")
            },
            Self::WriteMessage(error) => {
                write!(
                    f,
                    "Couldn't write message to Unix domain socket: {error}"
                )
            },
        }
    }
}
