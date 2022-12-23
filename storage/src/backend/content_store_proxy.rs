// Copyright 2022 Ant Group. All rights reserved.

// SPDX-License-Identifier: Apache-2.0

// ! Storage backend driver to access blobs on the local containerd content store using a unix socket (tcp) proxy.

use nydus_utils::metrics::BackendMetrics;

use crate::utils::alloc_buf;

use super::{BackendError, BackendResult, BlobBackend, BlobReader};
use std::{
    fmt::Display,
    io::{Error, Read, Result, Write},
    num::ParseIntError,
    os::unix::net::UnixStream,
    path::Path,
    str::{self, Utf8Error},
    sync::Arc,
};

#[derive(Debug)]
pub enum ContentStoreProxyError {
    /// Failed to create a unix socket stream.
    CreateUnixSocketStream(Error),
    /// Failed to write to a unix socket stream.
    WriteToUnixSocketStream(Error),
    /// Failed to read from a unix socket stream.
    ReadFromUnixSocketStream(Error),
    /// Failed to convert bytes to string.
    ConvertBytesToString(Utf8Error),
    /// Failed to parse string to integer.
    ParseStringToInt(ParseIntError),
    /// Failed to set blob type.
    SetBlobType,
}

impl From<ContentStoreProxyError> for BackendError {
    fn from(error: ContentStoreProxyError) -> Self {
        BackendError::ContentStoreProxy(error)
    }
}

const MAX_SIZE_STR_LEN: usize = 20;

// the commands used to communicate with the proxy
const SET_TYPE_COMMAND: &str = "set_type";
const BLOB_SIZE_COMMAND: &str = "blob_size";
const READ_COMMAND: &str = "read";

/// ContentStoreProxy is a storage backend driver to access blobs stored in the local containerd content store
/// through a unix socket (tcp) proxy which is implemented in the containerd/nydus-snapshotter.
pub struct ContentStoreProxy {
    socket_path: String,
    blob_type: CsProxyContentType,
    metrics: Option<Arc<BackendMetrics>>,
}

/// ContentStoreProxyReader is a BlobReader to implement the ContentStoreProxy backend driver.
pub struct ContentStoreProxyReader {
    client: Arc<UnixStream>,
    metrics: Arc<BackendMetrics>,
}

/// CsProxyContentType indicates that which part of the blob layer should be read by the ContentStoreProxyReader.
pub enum CsProxyContentType {
    /// WholeBlob means that the reader read the whole blob layer.
    WholeBlob,
    /// RawBlob means that the reader only read the raw blob data of the blob layer.
    RawBlob,
    /// Bootstrap means that the reader only read the bootstrap data of the blob layer.
    Bootstrap,
}

impl Display for CsProxyContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsProxyContentType::WholeBlob => write!(f, "whole"),
            CsProxyContentType::RawBlob => write!(f, "raw"),
            CsProxyContentType::Bootstrap => write!(f, "bootstrap"),
        }
    }
}

impl ContentStoreProxyReader {
    fn communicate(&self, req: &str, resp: &mut [u8]) -> BackendResult<usize> {
        let mut req = req.as_bytes().to_vec();
        req.push(b'&');
        self.client
            .as_ref()
            .write_all(&req)
            .map_err(ContentStoreProxyError::WriteToUnixSocketStream)?;

        let size = self
            .client
            .as_ref()
            .read(resp)
            .map_err(ContentStoreProxyError::ReadFromUnixSocketStream)?;

        println!("received {} bytes from proxy", size);

        Ok(size)
    }

    fn communicate_resp(&self, req: &str, resp_len: usize) -> BackendResult<String> {
        let mut resp = alloc_buf(resp_len);
        let size = self.communicate(req, &mut resp)?;
        let resp = str::from_utf8(&resp[..size])
            .map_err(ContentStoreProxyError::ConvertBytesToString)?
            .trim_end_matches('\x00')
            .to_string();
        Ok(resp)
    }

    fn set_type(&self, blob_type: &CsProxyContentType) -> BackendResult<()> {
        let req = format!("{};{}", SET_TYPE_COMMAND, blob_type);
        match self.communicate_resp(req.as_str(), 2) {
            Ok(resp) => {
                if resp == "ok" {
                    Ok(())
                } else {
                    Err(ContentStoreProxyError::SetBlobType.into())
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl BlobReader for ContentStoreProxyReader {
    // Note! If the blob is compressed, the function will always return 0
    // as we cannot get the actual size by stream reading.
    fn blob_size(&self) -> super::BackendResult<u64> {
        let size = self
            .communicate_resp(BLOB_SIZE_COMMAND, MAX_SIZE_STR_LEN)?
            .parse::<u64>()
            .map_err(ContentStoreProxyError::ParseStringToInt)?;
        Ok(size)
    }

    fn try_read(&self, buf: &mut [u8], offset: u64) -> BackendResult<usize> {
        let mut need_to_read_size = buf.len();
        let mut recv_size = 0;
        loop {
            let req = format!(
                "{};{};{}",
                READ_COMMAND,
                offset + (recv_size as u64),
                need_to_read_size,
            );
            let size = self.communicate(req.as_str(), &mut buf[recv_size..])?;
            if size == 0 {
                break;
            }
            recv_size += size;
            need_to_read_size -= size;
            if need_to_read_size == 0 {
                break;
            }
            println!(
                "offset {}, need to read {} bytes",
                offset + (recv_size as u64),
                need_to_read_size,
            );
        }
        Ok(recv_size)
    }

    fn metrics(&self) -> &nydus_utils::metrics::BackendMetrics {
        &self.metrics
    }
}

impl ContentStoreProxy {
    pub fn new(
        socket_path: &str,
        blob_type: CsProxyContentType,
        id: Option<&str>,
    ) -> Result<ContentStoreProxy> {
        Ok(ContentStoreProxy {
            socket_path: socket_path.to_string(),
            blob_type,
            metrics: id.map(|i| BackendMetrics::new(i, "content-store-proxy")),
        })
    }
}

impl BlobBackend for ContentStoreProxy {
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
        let unix_stream = UnixStream::connect(Path::new(self.socket_path.as_str()))
            .map_err(ContentStoreProxyError::CreateUnixSocketStream)?;
        let reader = Arc::new(ContentStoreProxyReader {
            client: Arc::new(unix_stream),
            metrics: self.metrics.as_ref().unwrap().clone(),
        });
        reader.set_type(&self.blob_type)?;
        Ok(reader)
    }
}

impl Drop for ContentStoreProxy {
    fn drop(&mut self) {
        self.shutdown();
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.release().unwrap_or_else(|e| error!("{:?}", e));
        }
    }
}

#[cfg(test)]
mod tests {}
