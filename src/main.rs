use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};

const SERVER: Token = Token(0);

fn handle_client(connection: &mut TcpStream) -> std::io::Result<bool> {
    let mut buffer = [0; 1024];
    match connection.read(&mut buffer) {
        Ok(0) => {
            // Connection closed by client
            eprintln!("Connection closed");
            Ok(true)
        }
        Ok(_n) => {
            // Echo data back to the client.
            connection.write_all("+PONG\r\n".as_bytes())?;
            // connection.write_all(&buffer[..n])?;
            Ok(false)
        }
        Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
            // Socket is not ready, try again later.
            eprintln!("Socket is not ready, try again later.: {}", e);

            Ok(false)
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            Err(e)
        }
    }
}

fn main() -> std::io::Result<()> {
    // Create a poll instance
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    // Set up the TCP listener
    let addr = "0.0.0.0:6379".parse().unwrap();
    let mut listener = TcpListener::bind(addr).unwrap();
    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)?;

    // Create a map to store the connected clients
    let mut clients = HashMap::new();

    // Start the event loop
    loop {
        poll.poll(&mut events, None)?;

        for event in &events {
            match event.token() {
                SERVER => {
                    let (mut stream, address) = listener.accept()?;
                    eprintln!("Accepted connection from: {}", address);

                    let token = Token(clients.len() + 1);
                    poll.registry()
                        .register(&mut stream, token, Interest::READABLE)?;
                    clients.insert(token, stream);
                }
                token => {
                    let done = if let Some(stream) = clients.get_mut(&token) {
                        handle_client(stream)
                    } else {
                        // Connection was closed
                        Ok(true)
                    };

                    if done? {
                        clients.remove(&token);
                    }
                }
            }
        }
    }
}
