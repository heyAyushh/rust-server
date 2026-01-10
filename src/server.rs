use crate::http::{Request, Response, StatusCode, request::ParseError};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::Semaphore};
use std::sync::Arc;
use std::thread;

pub struct Server {
    addr: String,
}

#[async_trait::async_trait]
pub trait Handler: Send + Sync {
    async fn handle_request(&self, request: &Request<'_>) -> Response;

    async fn handle_bad_request(&self, e: &ParseError) -> Response {
        println!("Failed to parse request: {}", e);
        Response::new(StatusCode::BadRequest, None)
    }
}

impl Server {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    pub async fn run<H: Handler + 'static>(self, handler: H) {
        println!("Server is running on {}", &self.addr);
        let listener = match TcpListener::bind(&self.addr).await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("Failed to bind server to {}: {}", &self.addr, e);
                return;
            }
        };
        let handler = Arc::new(handler);
        
        // Limit concurrent connections based on available CPU cores to maximize performance
        // Using a multiplier to allow for I/O-bound workloads while preventing resource exhaustion
        let cpu_count = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let max_connections = cpu_count * 100;
        let connection_limit = Arc::new(Semaphore::new(max_connections));
        
        println!("Server configured for {} available CPU cores with {} max concurrent connections", 
                 cpu_count, max_connections);

        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let handler = Arc::clone(&handler);
                    let permit = match connection_limit.clone().acquire_owned().await {
                        Ok(permit) => permit,
                        Err(e) => {
                            eprintln!("Failed to acquire connection permit: {}", e);
                            continue;
                        }
                    };
                    
                    tokio::spawn(async move {
                        let _permit = permit; // Hold permit until task completes
                        let mut buffer = [0; 1024];
                        match stream.read(&mut buffer).await {
                            Ok(_) => {
                                println!("We have a request. {}", String::from_utf8_lossy(&buffer));
                                let response = match Request::try_from(&buffer[..]) {
                                    Ok(request) => handler.handle_request(&request).await,
                                    Err(e) => handler.handle_bad_request(&e).await,
                                };

                                if let Err(e) = response.send(&mut stream).await {
                                    println!("Failed to send Response: {}", e);
                                }
                            }
                            Err(e) => {
                                println!("Failed to read from connection: {}", e)
                            }
                        }
                    });
                }
                Err(e) => {
                    println!("Failed to establish a connection: {}", e)
                }
            }
        }
    }
}
