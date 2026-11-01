use std::sync::{Arc, PoisonError, RwLock};

use http::HeaderValue;

/// The `Authorization` header requests to the cache are authenticated with.
///
/// Clones share the same underlying state.
#[derive(Clone)]
pub(crate) struct Token {
    authorization: Arc<RwLock<HeaderValue>>,
}

impl Token {
    pub(crate) fn authorization(&self) -> HeaderValue {
        self.authorization
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn new(authorization: HeaderValue) -> Self {
        Self { authorization: Arc::new(RwLock::new(authorization)) }
    }

    pub(crate) fn update(&self, authorization: HeaderValue) {
        *self.authorization.write().unwrap_or_else(PoisonError::into_inner) =
            authorization;
    }
}
