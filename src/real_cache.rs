use core::fmt;
use core::num::{NonZeroU16, NonZeroU64};
use core::pin::pin;
use core::str::FromStr;
use core::time::Duration;
use std::collections::VecDeque;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures::AsyncRead;
use nix_types::{NarFileName, NarInfo, NarInfoFileName, StoreHash};
use reqwest::{Method, StatusCode, header};
use smallvec::SmallVec;
use sub60_cache_api::endpoints::{
    missing_narinfos,
    nar_multiparts,
    nar_multiparts_complete,
    nar_multiparts_parts,
};
use sub60_cache_api::{CacheName, Etag, NamespaceName};

use crate::async_read_ext::AsyncReadExt;
use crate::context;
use crate::protocol::StoreDir;
use crate::run::RunArgs;
use crate::token::Token;

#[derive(Clone)]
pub(crate) struct RealCache {
    cache_url: url::Url,
    client: reqwest::Client,
}

#[derive(Debug, clap::Args)]
pub(crate) struct RealCacheRunArgs {
    #[arg(long)]
    namespace: NamespaceName,
    #[arg(long)]
    cache: CacheName,
}

#[derive(Debug, derive_more::Display, cauchy::Error)]
pub(crate) enum RealCacheConnectError {
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
    #[display("cache didn't return an upload URL for part {_0}")]
    MissingPartUrl(NonZeroU16),
    #[display("missing {header} header from {method} {url}")]
    MissingResponseHeader {
        method: Method,
        url: url::Url,
        header: header::HeaderName,
    },
    #[display("couldn't parse missing NARInfos response body")]
    ParseMissingNarInfosResponseBody(
        #[source] <missing_narinfos::ResponseBody as FromStr>::Err,
    ),
    #[display("couldn't parse NAR multipart response body")]
    ParseNarMultipartsResponseBody(
        #[source] <nar_multiparts::ResponseBody as FromStr>::Err,
    ),
    #[display("couldn't parse multipart parts response body")]
    ParsePartsResponseBody(
        #[source] <nar_multiparts_parts::ResponseBody as FromStr>::Err,
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

pub(crate) struct ResponseBodySuffix<'a>(pub(crate) &'a str);

impl fmt::Display for ResponseBodySuffix<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            Ok(())
        } else {
            write!(formatter, ": {}", self.0)
        }
    }
}

impl RealCache {
    /// TODO: docs.
    pub(crate) async fn connect(
        RunArgs { cache_args: args, origin, .. }: &RunArgs,
    ) -> Result<Self, RealCacheConnectError> {
        let mut cache_url = origin.clone();
        cache_url
            .path_segments_mut()
            .expect("cache URL can be a base")
            .push(&args.namespace)
            .push(&args.cache);

        let client = reqwest::Client::builder()
            .build()
            .map_err(RealCacheConnectError::BuildHttpClient)?;

        Ok(Self { cache_url, client })
    }

    /// Initiates a multipart upload for a NAR of the given size, returning
    /// `None` if the cache already contains it.
    async fn initiate_multipart_nar_upload(
        &self,
        token: &Token,
        nar_file_name: &NarFileName,
        nar_size: NonZeroU64,
    ) -> Result<Option<nar_multiparts::ResponseBody>, CacheRequestError> {
        let url = self.nar_multiparts_url();

        let response = self
            .client
            .post(url.clone())
            .header(header::AUTHORIZATION, token.authorization())
            .body(
                nar_multiparts::RequestBody {
                    nar_file_name: nar_file_name.clone(),
                    file_size: nar_size,
                }
                .to_string(),
            )
            .send()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::POST,
                source,
            })?;

        match response.status() {
            StatusCode::OK => {},
            StatusCode::NO_CONTENT => return Ok(None),
            _ => {
                return Err(CacheRequestError::unexpected_response(
                    response,
                    Method::POST,
                    url,
                )
                .await);
            },
        }

        response
            .text()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::POST,
                source,
            })?
            .parse::<nar_multiparts::ResponseBody>()
            .map(Some)
            .map_err(CacheRequestError::ParseNarMultipartsResponseBody)
    }

    async fn complete_multipart_nar_upload(
        &self,
        token: &Token,
        nar_file_name: NarFileName,
        upload_id: &nar_multiparts::UploadId,
        etags: SmallVec<[Etag; 8]>,
    ) -> Result<bool, CacheRequestError> {
        let url = self.nar_multiparts_complete_url(upload_id);

        let response = self
            .client
            .post(url.clone())
            .header(header::AUTHORIZATION, token.authorization())
            .body(
                nar_multiparts_complete::RequestBody { etags, nar_file_name }
                    .to_string(),
            )
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

    fn missing_narinfos_url(&self) -> url::Url {
        let mut url = self.cache_url.clone();
        url.path_segments_mut()
            .expect("cache URL can be a base")
            .push("missing-narinfos");
        url
    }

    fn nar_multiparts_url(&self) -> url::Url {
        let mut url = self.cache_url.clone();
        url.path_segments_mut()
            .expect("cache URL can be a base")
            .push("nar-multiparts");
        url
    }

    fn nar_multiparts_parts_url(
        &self,
        upload_id: &nar_multiparts::UploadId,
    ) -> url::Url {
        let mut url = self.nar_multiparts_url();
        url.path_segments_mut()
            .expect("URL can be a base")
            .push(upload_id)
            .push("parts");
        url
    }

    fn nar_multiparts_complete_url(
        &self,
        upload_id: &nar_multiparts::UploadId,
    ) -> url::Url {
        let mut url = self.nar_multiparts_url();
        url.path_segments_mut()
            .expect("URL can be a base")
            .push(upload_id)
            .push("complete");
        url
    }

    async fn request_part_urls(
        &self,
        token: &Token,
        upload_id: &nar_multiparts::UploadId,
        part_numbers: SmallVec<[NonZeroU16; 8]>,
        nar_file_name: NarFileName,
    ) -> Result<nar_multiparts_parts::ResponseBody, CacheRequestError> {
        let url = self.nar_multiparts_parts_url(upload_id);

        let response = self
            .client
            .post(url.clone())
            .header(header::AUTHORIZATION, token.authorization())
            .body(
                nar_multiparts_parts::RequestBody {
                    part_numbers,
                    nar_file_name,
                }
                .to_string(),
            )
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
            .parse::<nar_multiparts_parts::ResponseBody>()
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
        token: &Token,
        nar_file_name: &NarFileName,
        nar_size: NonZeroU64,
        initiate_body: &nar_multiparts::ResponseBody,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<SmallVec<[Etag; 8]>, CacheRequestError> {
        let part_size = initiate_body.part_size.get() as usize;

        // The cache only gives out URLs for the parts the NAR's size has.
        let num_parts = u16::try_from(
            nar_size.get().div_ceil(initiate_body.part_size.get().into()),
        )
        .map_err(|_| CacheRequestError::TooManyNarParts)?;

        let mut part_urls =
            initiate_body.parts_urls.iter().cloned().collect::<VecDeque<_>>();

        let mut urls_expiration_ts = initiate_body.expiration_ts;

        let mut nar_bytes = pin!(nar_bytes);
        let mut etags = SmallVec::new();
        let mut part_bytes_buf = BytesMut::zeroed(part_size);

        for part_number in (1..=num_parts).filter_map(NonZeroU16::new) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("we're past the UNIX epoch");

            // Discard the current upload URLs if they're past their expiration
            // timestamp, with some safety buffer to account for clock skew and
            // network delay.
            if now + Duration::from_secs(10) >= urls_expiration_ts {
                part_urls.clear();
            }

            if part_urls.is_empty() {
                let new_part_numbers = (part_number.get()..=num_parts)
                    .take(4)
                    .filter_map(NonZeroU16::new)
                    .collect::<SmallVec<_>>();
                let parts = self
                    .request_part_urls(
                        token,
                        &initiate_body.upload_id,
                        new_part_numbers,
                        nar_file_name.clone(),
                    )
                    .await?;
                part_urls.extend(parts.urls);
                urls_expiration_ts = parts.expiration_ts;
            }

            let part_url = part_urls
                .pop_front()
                .ok_or(CacheRequestError::MissingPartUrl(part_number))?;

            let num_read = nar_bytes
                .as_mut()
                .try_fill(&mut part_bytes_buf[..])
                .await
                .map_err(CacheRequestError::ReadNar)?;

            part_bytes_buf.truncate(num_read);

            let part_bytes = part_bytes_buf.freeze();

            let etag =
                self.upload_nar_part(part_url, part_bytes.clone()).await?;

            etags.push(etag);

            part_bytes_buf = part_bytes
                .try_into_mut()
                .unwrap_or_else(|_| BytesMut::zeroed(part_size));
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

impl context::Cache for RealCache {
    type Error = CacheRequestError;

    async fn missing_narinfos(
        &self,
        token: &Token,
        store_hashes: &[StoreHash],
    ) -> Result<Vec<StoreHash>, Self::Error> {
        let url = self.missing_narinfos_url();

        let mut missing = Vec::new();

        for chunk in store_hashes.chunks(missing_narinfos::MAX_STORE_HASHES) {
            let response = self
                .client
                .post(url.clone())
                .header(header::AUTHORIZATION, token.authorization())
                .body(missing_narinfos::StoreHashes(chunk.to_vec()).to_string())
                .send()
                .await
                .map_err(|source| CacheRequestError::HttpRequest {
                    method: Method::POST,
                    source,
                })?;

            if response.status() != StatusCode::OK {
                return Err(CacheRequestError::unexpected_response(
                    response,
                    Method::POST,
                    url,
                )
                .await);
            }

            let body = response
                .text()
                .await
                .map_err(|source| CacheRequestError::HttpRequest {
                    method: Method::POST,
                    source,
                })?
                .parse::<missing_narinfos::ResponseBody>()
                .map_err(CacheRequestError::ParseMissingNarInfosResponseBody)?;

            missing.extend(body.0);
        }

        Ok(missing)
    }

    async fn upload_nar(
        &self,
        token: &Token,
        nar_file_name: NarFileName,
        nar_size: NonZeroU64,
        nar_bytes: impl AsyncRead + Send + 'static,
    ) -> Result<u64, Self::Error> {
        let Some(initiate_body) = self
            .initiate_multipart_nar_upload(token, &nar_file_name, nar_size)
            .await?
        else {
            return Ok(0);
        };

        let etags = self
            .upload_nar_multiparts(
                token,
                &nar_file_name,
                nar_size,
                &initiate_body,
                nar_bytes,
            )
            .await?;

        let did_insert = self
            .complete_multipart_nar_upload(
                token,
                nar_file_name,
                &initiate_body.upload_id,
                etags,
            )
            .await?;

        Ok(if did_insert { nar_size.get() } else { 0 })
    }

    async fn upload_narinfo(
        &self,
        token: &Token,
        narinfo_filename: NarInfoFileName,
        narinfo: NarInfo<(), StoreDir>,
    ) -> Result<u64, Self::Error> {
        let narinfo = narinfo.with_url("").to_string();

        let narinfo_size = narinfo.len() as u64;

        let url = self.narinfo_url(&narinfo_filename);

        let response = self
            .client
            .put(url.clone())
            .header(header::AUTHORIZATION, token.authorization())
            .body(narinfo)
            .send()
            .await
            .map_err(|source| CacheRequestError::HttpRequest {
                method: Method::PUT,
                source,
            })?;

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
