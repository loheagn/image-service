// Copyright 2022 Ant Group. All rights reserved.

// SPDX-License-Identifier: Apache-2.0

// ! Storage backend driver to access the blobs on the local machine using a http proxy over unix socket.

use http::{Method, Request};
use hyper::{body, Body, Client, Response};
use hyperlocal::{UnixClientExt, UnixConnector, Uri};
use nydus_api::LocalHttpProxyConfig;
use nydus_utils::metrics::BackendMetrics;
use tokio::runtime::Runtime;

use super::{BackendError, BackendResult, BlobBackend, BlobReader};
use std::{
    fmt,
    io::{Error, Result},
    num::ParseIntError,
    str::{self},
    sync::Arc,
};

#[derive(Debug)]
pub enum LocalHttpProxyError {
    /// Failed to parse string to integer.
    ParseStringToInteger(ParseIntError),
    ParseContentLengthFromHeader(http::header::ToStrError),
    /// Failed to get response from the local http server.
    Request(hyper::Error),
    /// Failed to build the tokio runtime.
    BuildTokioRuntime(Error),
    /// Failed to build local http request.
    BuildHttpRequest(http::Error),
    /// Failed to read the response body.
    ReadResponseBody(hyper::Error),
    /// Failed to copy the buffer.
    CopyBuffer(Error),
}

impl fmt::Display for LocalHttpProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocalHttpProxyError::ParseStringToInteger(e) => {
                write!(f, "failed to parse string to integer, {}", e)
            }
            LocalHttpProxyError::ParseContentLengthFromHeader(e) => {
                write!(f, "failed to parse content length from header, {}", e)
            }
            LocalHttpProxyError::Request(e) => write!(f, "failed to get response, {}", e),
            LocalHttpProxyError::BuildTokioRuntime(e) => {
                write!(f, "failed to build tokio runtime, {}", e)
            }
            LocalHttpProxyError::BuildHttpRequest(e) => {
                write!(f, "failed to build http request, {}", e)
            }
            LocalHttpProxyError::ReadResponseBody(e) => {
                write!(f, "failed to read response body, {}", e)
            }
            LocalHttpProxyError::CopyBuffer(e) => write!(f, "failed to copy buffer, {}", e),
        }
    }
}

impl From<LocalHttpProxyError> for BackendError {
    fn from(error: LocalHttpProxyError) -> Self {
        BackendError::LocalHttpProxy(error)
    }
}

/// A storage backend driver to access blobs stored in the local machine
/// through a http proxy server over unix socket.
///
/// `LocalHttpProxy` uses two API endpoints to access the blobs:
/// - `HEAD /` to get the blob size
/// - `GET /` to read the blob
///
/// The http proxy server should respect [the `Range` header](https://www.rfc-editor.org/rfc/rfc9110.html#name-range) to compute the offset and length of the blob.
pub struct LocalHttpProxy {
    socket_path: String,
    thread_num: usize,
    metrics: Option<Arc<BackendMetrics>>,
}

/// LocalHttpProxyReader is a BlobReader to implement the LocalHttpProxy backend driver.
pub struct LocalHttpProxyReader {
    uri: Arc<hyper::Uri>,
    client: Arc<Client<UnixConnector>>,
    runtime: Arc<Runtime>,
    metrics: Arc<BackendMetrics>,
}

fn range_str_for_header(offset: u64, len: Option<usize>) -> String {
    match len {
        Some(len) => format!("bytes={}-{}", offset, offset + len as u64 - 1),
        None => format!("bytes={}-", offset),
    }
}

fn build_tokio_runtime(name: &str, thread_num: usize) -> BackendResult<Runtime> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name(name)
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .map_err(LocalHttpProxyError::BuildTokioRuntime)?;
    Ok(runtime)
}

impl LocalHttpProxyReader {
    async fn do_req(
        &self,
        only_head: bool,
        offset: u64,
        len: Option<usize>,
    ) -> BackendResult<Response<Body>> {
        let method = if only_head { Method::HEAD } else { Method::GET };
        let req = Request::builder()
            .method(method)
            .uri(self.uri.as_ref())
            .header(http::header::RANGE, range_str_for_header(offset, len))
            .body(Body::default())
            .map_err(LocalHttpProxyError::BuildHttpRequest)?;
        let resp = self
            .client
            .request(req)
            .await
            .map_err(LocalHttpProxyError::Request)?;
        Ok(resp)
    }

    async fn blob_size_async(&self) -> BackendResult<u64> {
        let content_length = self.do_req(true, 0, None).await?.headers()
            [http::header::CONTENT_LENGTH]
            .to_str()
            .map_err(LocalHttpProxyError::ParseContentLengthFromHeader)?
            .parse::<u64>()
            .map_err(LocalHttpProxyError::ParseStringToInteger)?;
        Ok(content_length)
    }

    async fn try_read_async(&self, offset: u64, len: usize) -> BackendResult<Vec<u8>> {
        let resp = self.do_req(false, offset, Some(len)).await?;
        let bytes = body::to_bytes(resp.into_body())
            .await
            .map_err(LocalHttpProxyError::ReadResponseBody)?;
        Ok(bytes.to_vec())
    }
}

impl BlobReader for LocalHttpProxyReader {
    // Note! If the blob is compressed, the function will always return 0
    // as we cannot get the actual size by stream reading.
    fn blob_size(&self) -> super::BackendResult<u64> {
        let size = self.runtime.block_on(self.blob_size_async())?;
        Ok(size)
    }

    fn try_read(&self, mut buf: &mut [u8], offset: u64) -> BackendResult<usize> {
        let content = self
            .runtime
            .block_on(self.try_read_async(offset, buf.len()))?;
        let copied_size = std::io::copy(&mut content.as_slice(), &mut buf)
            .map_err(LocalHttpProxyError::CopyBuffer)?;
        Ok(copied_size as usize)
    }

    fn metrics(&self) -> &nydus_utils::metrics::BackendMetrics {
        &self.metrics
    }
}

impl LocalHttpProxy {
    pub fn new(config: &LocalHttpProxyConfig, id: Option<&str>) -> Result<LocalHttpProxy> {
        Ok(LocalHttpProxy {
            socket_path: config.socket_path.to_string(),
            thread_num: config.thread_num,
            metrics: id.map(|i| BackendMetrics::new(i, "local-http-proxy")),
        })
    }
}

impl BlobBackend for LocalHttpProxy {
    fn shutdown(&self) {
        // do nothing
    }

    fn metrics(&self) -> &nydus_utils::metrics::BackendMetrics {
        // `metrics()` is only used for nydusd, which will always provide valid `blob_id`, thus
        // `self.metrics` has valid value.
        self.metrics.as_ref().unwrap()
    }

    fn get_reader(
        &self,
        _blob_id: &str,
    ) -> super::BackendResult<std::sync::Arc<dyn super::BlobReader>> {
        let client = Client::unix();
        let runtime = build_tokio_runtime("local-http-proxy", self.thread_num)?;
        let reader = Arc::new(LocalHttpProxyReader {
            uri: Arc::new(Uri::new(self.socket_path.clone(), "/").into()),
            client: Arc::new(client),
            runtime: Arc::new(runtime),
            metrics: self.metrics.as_ref().unwrap().clone(),
        });
        Ok(reader)
    }
}

impl Drop for LocalHttpProxy {
    fn drop(&mut self) {
        self.shutdown();
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.release().unwrap_or_else(|e| error!("{:?}", e));
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        backend::{local_http_proxy::LocalHttpProxy, BlobBackend},
        utils::alloc_buf,
    };

    use http::status;
    use hyper::{
        service::{make_service_fn, service_fn},
        Body, Response, Server,
    };
    use hyperlocal::UnixServerExt;
    use nydus_api::LocalHttpProxyConfig;
    use std::{
        cmp,
        fs::{self, File},
        io::Write,
        path::Path,
        thread,
        time::Duration,
    };

    use super::build_tokio_runtime;

    const CONTENT: &str = "some content for test";
    const SOCKET_PATH: &str = "/tmp/nydus-test-local-http-proxy.sock";

    fn parse_range_header(range_str: &str) -> (u64, Option<u64>) {
        let range_str = range_str.trim_start_matches("bytes=");
        let range: Vec<&str> = range_str.split('-').collect();
        let start = range[0].parse::<u64>().unwrap();
        let end = match range[1] {
            "" => None,
            _ => Some(cmp::min(
                range[1].parse::<u64>().unwrap(),
                (CONTENT.len() - 1) as u64,
            )),
        };
        (start, end)
    }

    #[test]
    fn test_head_and_get() {
        // create a temp file for test and write some content
        let src_file_path = Path::new("/tmp/nydus-test-local-http-proxy.src.txt");
        if src_file_path.exists() {
            fs::remove_file(src_file_path).unwrap();
        }
        File::create(src_file_path)
            .unwrap()
            .write_all(CONTENT.as_bytes())
            .unwrap();

        thread::spawn(|| {
            let rt = build_tokio_runtime("test-local-http-proxy-server", 1).unwrap();
            rt.block_on(async {
                println!("\nstarting server......");
                let path = Path::new(SOCKET_PATH);
                if path.exists() {
                    fs::remove_file(path).unwrap();
                }
                Server::bind_unix(path)
                    .unwrap()
                    .serve(make_service_fn(|_| async {
                        Ok::<_, hyper::Error>(service_fn(|req| async move {
                            let range = req.headers()[http::header::RANGE].to_str().unwrap();
                            println!("range: {}", range);
                            let (start, end) = parse_range_header(range);
                            let length = match end {
                                Some(e) => e - start + 1,
                                None => CONTENT.len() as u64,
                            };
                            println!("start: {}, end: {:?}, length: {}", start, end, length);

                            return match *req.method() {
                                hyper::Method::HEAD => Ok::<_, hyper::Error>(
                                    Response::builder()
                                        .status(200)
                                        .header(http::header::CONTENT_LENGTH, length)
                                        .body(Body::empty())
                                        .unwrap(),
                                ),
                                hyper::Method::GET => {
                                    let end = match end {
                                        Some(e) => e,
                                        None => (CONTENT.len() - 1) as u64,
                                    };
                                    let content = CONTENT.as_bytes()
                                        [start as usize..(end + 1) as usize]
                                        .to_vec();
                                    Ok::<_, hyper::Error>(
                                        Response::builder()
                                            .status(200)
                                            .header(http::header::CONTENT_LENGTH, length)
                                            .body(Body::from(content))
                                            .unwrap(),
                                    )
                                }
                                _ => Ok::<_, hyper::Error>(
                                    Response::builder()
                                        .status(status::StatusCode::METHOD_NOT_ALLOWED)
                                        .body(Body::empty())
                                        .unwrap(),
                                ),
                            };
                        }))
                    }))
                    .await
                    .unwrap();
            });
        });

        // wait for server to start
        thread::sleep(Duration::from_secs(5));

        // start the client and test
        let cfg = LocalHttpProxyConfig {
            socket_path: SOCKET_PATH.to_string(),
            thread_num: 1,
        };
        let backend = LocalHttpProxy::new(&cfg, Some("test-local-http-proxy")).unwrap();
        let reader = backend.get_reader("blob_id").unwrap();

        println!();
        println!("testing blob_size()......");
        let blob_size = reader
            .blob_size()
            .map_err(|e| {
                println!("blob_size() failed: {}", e);
                e
            })
            .unwrap();
        assert_eq!(blob_size, CONTENT.len() as u64);

        println!();
        println!("testing read() range......");
        let mut buf = alloc_buf(3);
        let size = reader
            .try_read(&mut buf, 0)
            .map_err(|e| {
                println!("read() range failed: {}", e);
                e
            })
            .unwrap();
        assert_eq!(size, 3);
        assert_eq!(buf, CONTENT.as_bytes()[0..3]);

        println!();
        println!("testing read() full......");
        let mut buf = alloc_buf(80);
        let size = reader
            .try_read(&mut buf, 0)
            .map_err(|e| {
                println!("read() range failed: {}", e);
                e
            })
            .unwrap();
        assert_eq!(size, CONTENT.len() as usize);
        assert_eq!(&buf[0..CONTENT.len()], CONTENT.as_bytes());
    }
}
