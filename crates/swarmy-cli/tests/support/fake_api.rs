use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub(crate) struct Request {
    pub method: String,
    pub authorization: Option<String>,
    pub path_and_query: String,
    pub body_len: usize,
    pub first_offset: Option<u64>,
}

pub(crate) fn serve(registered: bool) -> (SocketAddr, Arc<Mutex<Vec<Request>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&requests);
    std::thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(mut connection) = connection else {
                break;
            };
            let mut bytes = Vec::new();
            let mut chunk = [0; 8192];
            let header_end = loop {
                let count = connection.read(&mut chunk).unwrap();
                if count == 0 {
                    break None;
                }
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    break Some(index + 4);
                }
            };
            let Some(header_end) = header_end else {
                continue;
            };
            let header = String::from_utf8_lossy(&bytes[..header_end]);
            let mut parts = header.lines().next().unwrap().split_whitespace();
            let method = parts.next().unwrap().to_owned();
            let path_and_query = parts.next().unwrap().to_owned();
            let content_length: usize = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|n| n.trim().parse().ok())
                })
                .unwrap_or(0);
            while bytes.len() - header_end < content_length {
                let count = connection.read(&mut chunk).unwrap();
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..count]);
            }
            let body = &bytes[header_end..];
            let first_offset = (method == "POST" && body.len() >= 8)
                .then(|| u64::from_le_bytes(body[..8].try_into().unwrap()));
            let authorization = header.lines().find_map(|line| {
                line.split_once(':').and_then(|(key, value)| {
                    key.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_owned())
                })
            });
            recorded.lock().unwrap().push(Request {
                authorization,
                method: method.clone(),
                path_and_query: path_and_query.clone(),
                body_len: body.len(),
                first_offset,
            });
            let tag = path_and_query.split('/').next_back().unwrap_or("v0");
            let (status, response) = if method == "GET"
                && path_and_query.starts_with("/v1/images/base-ubuntu/")
                && registered
            {
                (
                    "200 OK",
                    serde_json::json!({"name":"base-ubuntu", "tag":tag, "manifest_id":"test"}),
                )
            } else if method == "POST" && path_and_query.starts_with("/v1/images/uploads") {
                (
                    "200 OK",
                    serde_json::json!({"name":"base-ubuntu", "tag":swarmy_version::RELEASE_TAG, "manifest_id":"test", "header":{"size":16_777_216,"chunk_size":262_144,"root_hash":"test"}, "size":16_777_216,"chunks_total":64,"chunks_stored":1,"chunks_uploaded":1}),
                )
            } else {
                (
                    "404 Not Found",
                    serde_json::json!({"code":"image_not_found", "message":"missing", "provider_text":null}),
                )
            };
            let response = response.to_string();
            let header = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            );
            connection.write_all(header.as_bytes()).unwrap();
            connection.write_all(response.as_bytes()).unwrap();
        }
    });
    (address, requests)
}
