//! Fetching the tokens requests to the cache are authenticated with.

use core::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use reqwest::header::{self, HeaderValue};
use sub60_cache_api::Endpoint as _;
use sub60_cache_api::endpoints::auth_github_actions;

use crate::context;
use crate::real_cache::ResponseBodySuffix;

/// How long to wait before each retry of an exchange that failed for a reason
/// that might be transient.
const EXCHANGE_RETRY_DELAYS: [Duration; 3] =
    [Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4)];

#[derive(Clone)]
pub(crate) enum RealTokenFetcher {
    /// A token given by the user.
    Token(HeaderValue),

    /// The tokens the cache gives in exchange for the OIDC tokens of the
    /// GitHub Actions job the daemon runs in.
    GitHubActions(GitHubActions),
}

#[derive(Debug, clap::Args)]
pub(crate) struct RealTokenFetcherArgs {
    /// The token to push store paths with. If unset, the GitHub Actions job's
    /// OIDC tokens are exchanged for the cache's tokens.
    #[arg(long, env = "SUB60_CACHE_TOKEN", hide_env_values = true)]
    token: Option<String>,
    #[arg(long, env = "ACTIONS_ID_TOKEN_REQUEST_URL")]
    oidc_token_request_url: Option<url::Url>,
    #[arg(
        long,
        env = "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
        hide_env_values = true
    )]
    oidc_token_request_token: Option<String>,
}

#[derive(Clone)]
pub(crate) struct GitHubActions {
    client: reqwest::Client,

    /// The URL the job's OIDC tokens are requested from.
    oidc_token_request_url: url::Url,

    /// The bearer token authenticating requests for the job's OIDC tokens.
    oidc_token_request_token: String,

    /// The origin of the cache's server.
    origin: url::Url,
}

#[derive(Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum NewRealTokenFetcherError {
    #[display("couldn't build HTTP client")]
    BuildHttpClient(#[source] reqwest::Error),
    #[display("the token isn't a valid header value")]
    InvalidToken(#[source] header::InvalidHeaderValue),
    #[display(
        "no token to push store paths with: set the `token` input, or give \
         the job the `id-token: write` permission"
    )]
    NoToken,
}

#[derive(Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum FetchTokenError {
    #[display("couldn't request the job's OIDC token")]
    RequestOidcToken(#[source] reqwest::Error),
    #[display(
        "unexpected response requesting the job's OIDC token: {status}{body}",
        body = ResponseBodySuffix(body),
    )]
    OidcTokenResponse { status: StatusCode, body: String },
    #[display("couldn't parse the job's OIDC token")]
    ParseOidcToken(#[source] serde_json::Error),
    #[display("couldn't exchange the job's OIDC token for a token")]
    Exchange(#[source] reqwest::Error),
    #[display(
        "unexpected response exchanging the job's OIDC token: {status}{body}",
        body = ResponseBodySuffix(body),
    )]
    ExchangeResponse { status: StatusCode, body: String },
    #[display(
        "couldn't parse the token the job's OIDC token was exchanged for"
    )]
    ParseExchange(#[source] serde_json::Error),
    #[display("the token isn't a valid header value")]
    InvalidToken(#[source] header::InvalidHeaderValue),
}

/// The body of a successful response to a request for a job's OIDC token.
#[derive(serde::Deserialize)]
struct OidcTokenResponseBody {
    value: String,
}

impl RealTokenFetcher {
    pub(crate) fn new(
        args: &RealTokenFetcherArgs,
        origin: url::Url,
    ) -> Result<Self, NewRealTokenFetcherError> {
        match args {
            RealTokenFetcherArgs { token: Some(token), .. } => {
                authorization(token)
                    .map(Self::Token)
                    .map_err(NewRealTokenFetcherError::InvalidToken)
            },
            RealTokenFetcherArgs {
                oidc_token_request_url: Some(url),
                oidc_token_request_token: Some(token),
                ..
            } => Ok(Self::GitHubActions(GitHubActions {
                client: reqwest::Client::builder()
                    .build()
                    .map_err(NewRealTokenFetcherError::BuildHttpClient)?,
                oidc_token_request_url: url.clone(),
                oidc_token_request_token: token.clone(),
                origin,
            })),
            _ => Err(NewRealTokenFetcherError::NoToken),
        }
    }
}

impl context::TokenFetcher for RealTokenFetcher {
    type Error = FetchTokenError;

    async fn fetch(
        &self,
    ) -> Result<(HeaderValue, Option<Duration>), Self::Error> {
        match self {
            Self::Token(authorization) => Ok((authorization.clone(), None)),
            Self::GitHubActions(github_actions) => github_actions
                .fetch()
                .await
                .map(|(authorization, expires_in)| {
                    (authorization, Some(expires_in))
                }),
        }
    }
}

impl GitHubActions {
    async fn fetch(&self) -> Result<(HeaderValue, Duration), FetchTokenError> {
        let mut retry_delays = EXCHANGE_RETRY_DELAYS.into_iter();

        let response = loop {
            match self.exchange().await {
                Ok(response) => break response,
                Err(err) if err.is_transient() => match retry_delays.next() {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => return Err(err),
                },
                Err(err) => return Err(err),
            }
        };

        let authorization = authorization(&response.token)
            .map_err(FetchTokenError::InvalidToken)?;

        Ok((authorization, response.expires_at.saturating_sub(unix_ts())))
    }

    /// Requests the job's OIDC token, and exchanges it for one of the cache's.
    async fn exchange(
        &self,
    ) -> Result<auth_github_actions::ResponseBody, FetchTokenError> {
        let mut oidc_token_url = self.oidc_token_request_url.clone();
        oidc_token_url.query_pairs_mut().append_pair(
            "audience",
            &self.origin.origin().ascii_serialization(),
        );

        let response = self
            .client
            .get(oidc_token_url)
            .bearer_auth(&self.oidc_token_request_token)
            .send()
            .await
            .map_err(FetchTokenError::RequestOidcToken)?;

        let status = response.status();

        let body =
            response.text().await.map_err(FetchTokenError::RequestOidcToken)?;

        if !status.is_success() {
            return Err(FetchTokenError::OidcTokenResponse { status, body });
        }

        let oidc_token = serde_json::from_str::<OidcTokenResponseBody>(&body)
            .map_err(FetchTokenError::ParseOidcToken)?
            .value;

        let exchange_url = self
            .origin
            .join(auth_github_actions::Endpoint::PATH)
            .expect("valid URL");

        let response = self
            .client
            .post(exchange_url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(
                auth_github_actions::RequestBody { token: oidc_token }
                    .to_string(),
            )
            .send()
            .await
            .map_err(FetchTokenError::Exchange)?;

        let status = response.status();

        let body = response.text().await.map_err(FetchTokenError::Exchange)?;

        if !status.is_success() {
            return Err(FetchTokenError::ExchangeResponse { status, body });
        }

        body.parse().map_err(FetchTokenError::ParseExchange)
    }
}

impl FetchTokenError {
    /// Whether trying again might succeed.
    fn is_transient(&self) -> bool {
        match self {
            Self::RequestOidcToken(_) | Self::Exchange(_) => true,
            Self::OidcTokenResponse { status, .. }
            | Self::ExchangeResponse { status, .. } => {
                status.is_server_error()
                    || *status == StatusCode::TOO_MANY_REQUESTS
            },
            Self::ParseOidcToken(_)
            | Self::ParseExchange(_)
            | Self::InvalidToken(_) => false,
        }
    }
}

/// Returns the `Authorization` header authenticating requests with the token.
fn authorization(
    token: &str,
) -> Result<HeaderValue, header::InvalidHeaderValue> {
    let mut authorization = HeaderValue::try_from(format!("Bearer {token}"))?;
    authorization.set_sensitive(true);
    Ok(authorization)
}

fn unix_ts() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("we're past the UNIX epoch")
}
