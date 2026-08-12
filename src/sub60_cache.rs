use core::num::{NonZeroU16, NonZeroU64};
use core::pin::pin;
use core::str::FromStr;
use core::time::Duration;
use core::{fmt, iter};
use std::collections::VecDeque;
use std::io;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, FutureExt};
use nix_types::{NarFileName, NarInfo, NarInfoFileName};
use reqwest::{Method, StatusCode, header};
use smallvec::SmallVec;
use sub60_cache::nar_multiparts::{
    CompleteRequestBody,
    Etag,
    NarMultipartsResponseBody,
    PartsRequestBody,
    PartsResponseBody,
    UploadId,
};
use sub60_cache::{CacheName, UserName};

use crate::async_read_ext::AsyncReadExt;
use crate::context;
use crate::protocol::StoreDir;
use crate::run::RunArgs;
use crate::tokio::TokioSpawner;

type AuthToken = String;

static SUB60_CACHE_URL: LazyLock<url::Url> =
    LazyLock::new(|| "https://cache.sub60.dev".parse().expect("valid URL"));

#[derive(Clone)]
pub(crate) struct Sub60Cache {
    cache_url: url::Url,
    client: reqwest::Client,
    spawner: TokioSpawner,
}

#[derive(Debug, clap::Args)]
pub(crate) struct Sub60CacheRunArgs {
    #[arg(long)]
    user: UserName,
    #[arg(long)]
    cache: CacheName,
    #[arg(long)]
    auth_token: AuthToken,
}

#[derive(Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum Sub60CacheConnectError {
    #[display("couldn't build HTTP client")]
    BuildHttpClient(#[source] reqwest::Error),
}

#[derive(Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum CacheRequestError {
    #[display("couldn't parse Etag")]
    ParseEtag(#[source] QuotedFromStrError<<Etag as FromStr>::Err>),
    #[display("invalid {header} header from {method} {url}")]
    InvalidResponseHeader {
        method: Method,
        url: url::Url,
        header: header::HeaderName,
        #[source]
        source: header::ToStrError,
    },
    #[display("missing {header} header from {method} {url}")]
    MissingResponseHeader {
        method: Method,
        url: url::Url,
        header: header::HeaderName,
    },
    #[display("couldn't parse NAR multipart response body")]
    ParseNarMultipartsResponseBody(
        #[source] <NarMultipartsResponseBody as core::str::FromStr>::Err,
    ),
    #[display("couldn't parse multipart parts response body")]
    ParsePartsResponseBody(
        #[source] <PartsResponseBody as core::str::FromStr>::Err,
    ),
    #[display("couldn't read NAR bytes")]
    ReadNar(#[source] io::Error),
    #[display("HTTP {method} request failed")]
    HttpRequest {
        method: Method,
        #[source]
        source: reqwest::Error,
    },
    #[display("NAR upload has too many multipart parts")]
    TooManyNarParts,
    #[display(
        "unexpected response from {method} {url}: {status}{body}",
        body = ResponseBodySuffix(body),
    )]
    UnexpectedResponse {
        method: Method,
        status: StatusCode,
        url: url::Url,
        body: String,
    },
}

#[derive(Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum QuotedFromStrError<T> {
    #[display("value must be wrapped in double quotes")]
    MissingQuotes,
    #[display("invalid quoted value")]
    Inner(#[source] T),
}

struct Quoted<T>(T);

struct ResponseBodySuffix<'a>(&'a str);

impl fmt::Display for ResponseBodySuffix<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            Ok(())
        } else {
            write!(formatter, ": {}", self.0)
        }
    }
}

impl Sub60Cache {
    /// TODO: docs.
    pub(crate) async fn connect(
        RunArgs { sub60_cache_args: args, .. }: &RunArgs,
        spawner: TokioSpawner,
    ) -> Result<Self, Sub60CacheConnectError> {
        let mut cache_url = SUB60_CACHE_URL.clone();
        cache_url
            .path_segments_mut()
            .expect("cache URL can be a base")
            .push(&args.user)
            .push(&args.cache);

        let client = reqwest::Client::builder()
            .build()
            .map_err(Sub60CacheConnectError::BuildHttpClient)?;

        Ok(Self { cache_url, client, spawner })
    }

    async fn initiate_multipart_nar_upload(
        &self,
        nar_file_name: &NarFileName,
    ) -> Result<NarMultipartsResponseBody, CacheRequestError> {
        let url = self.nar_multiparts_url();

        let response = self
            .client
            .post(url.clone())
            .body(nar_file_name.to_string())
            .send()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::POST,
                source,
            })?;

        if !response.status().is_success() {
            return Err(CacheRequestError::unexpected_response(
                response,
                Method::POST,
                url,
            )
            .await);
        }

        response
            .text()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::POST,
                source,
            })?
            .parse::<NarMultipartsResponseBody>()
            .map_err(CacheRequestError::ParseNarMultipartsResponseBody)
    }

    async fn complete_multipart_nar_upload(
        &self,
        nar_file_name: NarFileName,
        upload_id: &UploadId,
        etags: SmallVec<[Etag; 8]>,
    ) -> Result<bool, CacheRequestError> {
        let url = self.nar_multiparts_complete_url(upload_id);

        let response = self
            .client
            .post(url.clone())
            .body(CompleteRequestBody { etags, nar_file_name }.to_string())
            .send()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::POST,
                source,
            })?;

        match response.status() {
            StatusCode::CREATED => Ok(true),
            StatusCode::OK => Ok(false),
            _ => Err(CacheRequestError::unexpected_response(
                response,
                Method::POST,
                url,
            )
            .await),
        }
    }

    fn narinfo_url(&self, narinfo_filename: &NarInfoFileName) -> url::Url {
        let mut url = self.cache_url.clone();
        url.path_segments_mut()
            .expect("cache URL can be a base")
            .push(&narinfo_filename.to_string());
        url
    }

    fn nar_multiparts_url(&self) -> url::Url {
        let mut url = self.cache_url.clone();
        url.path_segments_mut()
            .expect("cache URL can be a base")
            .push("nar-multiparts");
        url
    }

    fn nar_multiparts_parts_url(&self, upload_id: &UploadId) -> url::Url {
        let mut url = self.nar_multiparts_url();
        url.path_segments_mut()
            .expect("URL can be a base")
            .push(upload_id)
            .push("parts");
        url
    }

    fn nar_multiparts_complete_url(&self, upload_id: &UploadId) -> url::Url {
        let mut url = self.nar_multiparts_url();
        url.path_segments_mut()
            .expect("URL can be a base")
            .push(upload_id)
            .push("complete");
        url
    }

    async fn request_part_urls(
        &self,
        upload_id: &UploadId,
        part_numbers: SmallVec<[NonZeroU16; 8]>,
        nar_file_name: NarFileName,
    ) -> Result<PartsResponseBody, CacheRequestError> {
        let url = self.nar_multiparts_parts_url(upload_id);

        let response = self
            .client
            .post(url.clone())
            .body(PartsRequestBody { part_numbers, nar_file_name }.to_string())
            .send()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::POST,
                source,
            })?;

        if !response.status().is_success() {
            return Err(CacheRequestError::unexpected_response(
                response,
                Method::POST,
                url,
            )
            .await);
        }

        response
            .text()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::POST,
                source,
            })?
            .parse::<PartsResponseBody>()
            .map_err(CacheRequestError::ParsePartsResponseBody)
    }

    async fn upload_nar_part(
        &self,
        url: url::Url,
        part_bytes: Bytes,
    ) -> Result<Etag, CacheRequestError> {
        let response = self
            .client
            .put(url.clone())
            .body(part_bytes)
            .send()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::PUT,
                source,
            })?;

        if !response.status().is_success() {
            return Err(CacheRequestError::unexpected_response(
                response,
                Method::PUT,
                url,
            )
            .await);
        }

        let etag = response.headers().get(header::ETAG).ok_or_else(|| {
            CacheRequestError::MissingResponseHeader {
                method: Method::PUT,
                url: url.clone(),
                header: header::ETAG,
            }
        })?;

        etag.to_str()
            .map_err(|source| CacheRequestError::InvalidResponseHeader {
                method: Method::PUT,
                url: url.clone(),
                header: header::ETAG,
                source,
            })?
            .parse::<Quoted<Etag>>()
            .map(|Quoted(etag)| etag)
            .map_err(CacheRequestError::ParseEtag)
    }

    async fn upload_nar_multiparts(
        &self,
        nar_file_name: &NarFileName,
        initiate_body: &NarMultipartsResponseBody,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<SmallVec<[Etag; 8]>, CacheRequestError> {
        let part_size = initiate_body.part_size.get() as usize;
        let mut part_urls = VecDeque::from(initiate_body.parts_urls.clone());
        let mut urls_expiration_ts = initiate_body.expiration_ts;

        let mut nar_bytes = pin!(nar_bytes);
        let mut part_number = NonZeroU16::MIN;
        let mut etags = SmallVec::new();
        let mut part_bytes_buf = BytesMut::zeroed(part_size);

        loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("we're past the UNIX epoch");

            // Discard the current upload URLs if they're past their expiration
            // timestamp, with some safety buffer to account for clock skew and
            // network delay.
            if now + Duration::from_secs(10) >= urls_expiration_ts {
                part_urls.clear();
            }

            let Some(part_url) = part_urls.pop_front() else {
                let new_part_numbers =
                    iter::successors(Some(part_number), |part_number| {
                        part_number.checked_add(1)
                    })
                    .take(4)
                    .collect::<SmallVec<_>>();
                let parts = self
                    .request_part_urls(
                        &initiate_body.upload_id,
                        new_part_numbers,
                        nar_file_name.clone(),
                    )
                    .await?;
                part_urls.extend(parts.urls);
                urls_expiration_ts = parts.expiration_ts;
                continue;
            };

            let num_read = nar_bytes
                .as_mut()
                .try_fill(&mut part_bytes_buf[..])
                .await
                .map_err(CacheRequestError::ReadNar)?;

            if num_read == 0 {
                break;
            }

            part_bytes_buf.truncate(num_read);

            let part_bytes = part_bytes_buf.freeze();

            let etag =
                self.upload_nar_part(part_url, part_bytes.clone()).await?;

            etags.push(etag);

            if num_read < part_size {
                break;
            }

            part_bytes_buf = part_bytes
                .try_into_mut()
                .unwrap_or_else(|_| BytesMut::zeroed(part_size));

            part_number = part_number
                .checked_add(1)
                .ok_or(CacheRequestError::TooManyNarParts)?;
        }

        Ok(etags)
    }
}

impl CacheRequestError {
    async fn unexpected_response(
        response: reqwest::Response,
        method: Method,
        url: url::Url,
    ) -> Self {
        CacheRequestError::UnexpectedResponse {
            method,
            status: response.status(),
            url,
            body: response.text().await.unwrap_or_else(|err| {
                format!("<couldn't read response body: {err}>")
            }),
        }
    }
}

impl context::Cache for Sub60Cache {
    type Error = CacheRequestError;

    fn has_narinfo(
        &self,
        narinfo_filename: &NarInfoFileName,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        let url = self.narinfo_url(narinfo_filename);

        #[expect(clippy::result_large_err)]
        let future =
            self.client.head(url.clone()).send().map(
                move |result| match result.map(|resp| resp.status()) {
                    Ok(StatusCode::OK) => Ok(true),
                    Ok(StatusCode::NOT_FOUND) => Ok(false),
                    Ok(status) => Err(CacheRequestError::UnexpectedResponse {
                        method: Method::HEAD,
                        status,
                        url,
                        body: String::new(),
                    }),
                    Err(source) => Err(CacheRequestError::HttpRequest {
                        method: Method::HEAD,
                        source,
                    }),
                },
            );

        // The caller will race this NARInfo lookup against the upstream caches
        // and intentionally drops the losing future. Dropping an in-flight
        // HTTP/2 request causes reqwest to terminate the corresponding stream
        // by sending an RST_STREAM frame. When handling many paths in rapid
        // succession, if the upstream lookups repeatedly win their races, this
        // produces a burst of resets on the single multiplexed connection to
        // cache.sub60.dev. Cloudflare treats that pattern as an HTTP/2 Rapid
        // Reset attack and closes the entire connection, taking unrelated
        // NARInfo lookups down with it (see
        // <https://blog.cloudflare.com/technical-breakdown-http2-rapid-reset-ddos-attack/>
        // for details).
        //
        // To prevent this, we let the lookup run to completion even if the
        // caller drops the future.
        self.spawner.run_to_completion(future)
    }

    async fn upload_nar(
        &self,
        nar_file_name: NarFileName,
        nar_size: NonZeroU64,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<u64, Self::Error> {
        let initiate_body =
            self.initiate_multipart_nar_upload(&nar_file_name).await?;

        let etags = self
            .upload_nar_multiparts(&nar_file_name, &initiate_body, nar_bytes)
            .await?;

        let did_insert = self
            .complete_multipart_nar_upload(
                nar_file_name,
                &initiate_body.upload_id,
                etags,
            )
            .await?;

        Ok(if did_insert { nar_size.get() } else { 0 })
    }

    async fn upload_narinfo(
        &self,
        narinfo_filename: NarInfoFileName,
        narinfo: NarInfo<(), StoreDir>,
    ) -> Result<u64, Self::Error> {
        let narinfo = narinfo.with_url("").to_string();

        let narinfo_size = narinfo.len() as u64;

        let url = self.narinfo_url(&narinfo_filename);

        let response =
            self.client.put(url.clone()).body(narinfo).send().await.map_err(
                |source| CacheRequestError::HttpRequest {
                    method: Method::PUT,
                    source,
                },
            )?;

        match response.status() {
            StatusCode::CREATED => Ok(narinfo_size),
            StatusCode::OK => Ok(0),
            _ => Err(CacheRequestError::unexpected_response(
                response,
                Method::PUT,
                url,
            )
            .await),
        }
    }
}

impl<T: FromStr> FromStr for Quoted<T> {
    type Err = QuotedFromStrError<T::Err>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .ok_or(QuotedFromStrError::MissingQuotes)?
            .parse()
            .map(Self)
            .map_err(QuotedFromStrError::Inner)
    }
}
