use core::fmt;
use core::str::FromStr;
use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, PoisonError, RwLock};

use http::HeaderValue;

/// A token requests to the cache are authenticated with.
#[derive(Clone)]
pub struct Token(String);

/// A token that can be updated, whose clones all share the same one.
#[derive(Clone)]
pub(crate) struct SharedToken(Arc<RwLock<Token>>);

#[derive(Debug, derive_more::Display, cauchy::Error)]
#[display("tokens must be one or more visible ASCII characters")]
pub struct InvalidTokenError;

impl Token {
    /// Returns the `Authorization` header authenticating requests with the
    /// token.
    pub(crate) fn authorization(&self) -> HeaderValue {
        let mut authorization =
            HeaderValue::try_from(format!("Bearer {}", self.0))
                .expect("tokens are visible ASCII");

        authorization.set_sensitive(true);
        authorization
    }
}

impl SharedToken {
    /// Returns the `Authorization` header authenticating requests with the
    /// current token.
    pub(crate) fn authorization(&self) -> HeaderValue {
        self.0.read().unwrap_or_else(PoisonError::into_inner).authorization()
    }

    pub(crate) fn new(token: Token) -> Self {
        Self(Arc::new(RwLock::new(token)))
    }

    pub(crate) fn update(&self, token: Token) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = token;
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token(..)")
    }
}

impl FromStr for Token {
    type Err = InvalidTokenError;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        // Visible ASCII makes a valid header value, and has no whitespace that
        // would split it in the netrc file.
        if !token.is_empty() && token.bytes().all(|b| b.is_ascii_graphic()) {
            Ok(Self(token.to_owned()))
        } else {
            Err(InvalidTokenError)
        }
    }
}

/// Writes the given token to the netrc file at the given path, which Nix reads
/// to authenticate its requests to the cache at the given origin.
pub(crate) fn write_token(token: &Token, origin: &url::Url, netrc_path: &Path) {
    let host = origin.host_str().expect("origins have a host");
    let tmp_path = netrc_path.with_extension("tmp");

    let write = || -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp_path)?;

        // The cache reads the token from the password, and ignores the login.
        writeln!(file, "machine {host} login sub60 password {}", token.0)?;

        // Replace the file in a single step so that Nix never reads a partially
        // written one.
        fs::rename(&tmp_path, netrc_path)
    };

    if let Err(err) = write() {
        eprintln!(
            "Couldn't write the token to {}: {err}",
            netrc_path.display()
        );
    }
}
