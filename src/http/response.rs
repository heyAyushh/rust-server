use tokio::io::{AsyncWriteExt, Result as IoResult};
use super::status_code::StatusCode;
pub struct Response {
    status_code: StatusCode,
    body: Option<String>
}

impl Response {
    pub fn new(status_code: StatusCode, body: Option<String>) -> Self {
        Response { status_code, body }
    }

    pub async fn send(&self, stream: &mut (impl AsyncWriteExt + Unpin)) -> IoResult<()> {
        let body = match &self.body {
            Some(b) => b,
            None => ""
        };

        let response = format!("HTTP/1.1 {} {}\r\n\r\n{}", self.status_code, self.status_code.reason_phrase(), body);
        stream.write_all(response.as_bytes()).await?;
        stream.flush().await
    }
}

