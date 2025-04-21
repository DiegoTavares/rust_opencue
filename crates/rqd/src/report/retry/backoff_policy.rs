use std::usize;

use super::{
    Outcome, Policy,
    backoff::{Backoff, ExponentialBackoff},
};
use futures::TryStreamExt;
// use futures_util::future;
use http::{Request, StatusCode, request::Parts};
use http_body_util::{BodyExt, Full};
use prost::bytes::Bytes;
// use hyper::body::Bytes;
use tonic::body::Body;
use tracing::warn;

type Req = http::Request<Body>;
type Res = http::Response<Body>;

#[derive(Clone)]
pub struct BackoffPolicy {
    /// Maximum number of retry attempts.
    /// If `None`, will retry indefinitely based on the policy logic.
    pub attempts: Option<usize>,

    /// The backoff strategy to use between retry attempts.
    /// Determines how long to wait before the next retry.
    pub backoff: ExponentialBackoff,
}

impl BackoffPolicy {
    /// Checks if there are still retry attempts left.
    ///
    /// This function determines whether more retry attempts can be made based on
    /// the configured maximum number of attempts.
    ///
    /// Returns:
    ///   - `true` if more retry attempts are allowed
    ///   - `false` if maximum attempts have been reached
    pub fn has_attempts_left(&mut self) -> bool {
        if self.attempts.unwrap_or(usize::MAX) > 0 {
            if let Some(remaining_attemps) = self.attempts {
                self.attempts.replace(remaining_attemps);
            }
            true
        } else {
            false
        }
    }
}

impl<E> Policy<Req, Res, E> for BackoffPolicy {
    // type Future = future::Ready<()>;
    type Future = tokio::time::Sleep;

    fn retry(&mut self, _req: &mut Req, result: Result<Res, E>) -> Outcome<Self::Future, Res, E> {
        match &result {
            Ok(response) => {
                if matches!(
                    response.status(),
                    StatusCode::INTERNAL_SERVER_ERROR
                        | StatusCode::BAD_GATEWAY
                        | StatusCode::SERVICE_UNAVAILABLE
                ) {
                    if self.has_attempts_left() {
                        warn!("Retrying for StatusCode={}", response.status());
                        Outcome::Retry(self.backoff.next_backoff())
                    } else {
                        Outcome::Return(result)
                    }
                } else {
                    Outcome::Return(result)
                }
            }
            Err(_err) => {
                if self.has_attempts_left() {
                    warn!("Retrying for Transport error.");
                    // Retry all transport errors
                    Outcome::Retry(self.backoff.next_backoff())
                } else {
                    Outcome::Return(result)
                }
            }
        }
    }

    fn clone_request(&mut self, req: Req) -> (Req, Option<Req>) {
        // Convert body to Bytes so it can be cloned
        let (parts, original_body) = req.into_parts();

        // Try to capture the Bytes from the original body
        // This is circumvoluted, I'm not sure how to call an async function within a sync function that is used inside a future later
        let bytes =
            futures::executor::block_on(async move { consume_unsync_body(original_body).await });

        // Re-create the request with the captured bytes in a new BoxBody
        let req = create_request(parts.clone(), bytes.clone());
        let cloned_req = create_request(parts, bytes);

        (req, Some(cloned_req))
        // Some(req.clone())
    }
}

/// Consume body stream and return its bytes
async fn consume_unsync_body(body: Body) -> Vec<u8> {
    body.into_data_stream()
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
        .unwrap()
}

/// Creates a new HTTP request using the provided parts and body data.
///
/// # Arguments
///
/// * `parts` - The HTTP request parts containing method, URI, headers, etc.
/// * `body` - The bytes that will form the body of the request
///
/// # Returns
///
/// A complete HTTP request with the specified parts and body.
fn create_request(parts: Parts, body: Vec<u8>) -> http::Request<Body> {
    let bytes = Bytes::from(body);
    let full_body = Full::new(bytes);
    let mut request = Request::builder()
        .method(parts.method)
        .uri(parts.uri)
        .version(parts.version)
        .body(Body::new(
            full_body
                .map_err(|_err| tonic::Status::internal("Body error"))
                .boxed(),
        ))
        .unwrap();

    *request.headers_mut() = parts.headers;

    request
}
