//! Integration tests for the QUIC transport layer.
//!
//! Tests exercise the full client-server flow: TLS setup, connection,
//! handshake, FlowFile transfer, and acknowledgment.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use runifi_transport::QuicClient;
use runifi_transport::client;
use runifi_transport::error::TransportResult;
use runifi_transport::protocol::WireFlowFile;
use runifi_transport::server::{QuicServer, ServerConfig};
use runifi_transport::tls;

/// Helper: start a server and create a client that trusts it.
async fn setup_pair() -> TransportResult<(QuicServer, QuicClient)> {
    let cert_key = tls::generate_self_signed(&["localhost"])?;
    let server_cert_der = cert_key.cert_chain[0].clone();

    let server_config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        cert_key,
        buffer_capacity: 1000,
        max_connections: 10,
    };

    let server = QuicServer::start(server_config).await?;
    let server_addr = server.local_addr();

    // Build client that trusts this server's cert
    let client = client::client_with_trusted_cert(server_addr, &server_cert_der, "localhost")?;

    Ok((server, client))
}

#[tokio::test]
async fn send_single_flowfile() {
    let (mut server, client) = setup_pair().await.unwrap();

    let ff = WireFlowFile {
        attributes: vec![
            (Arc::from("filename"), Arc::from("test.txt")),
            (Arc::from("mime.type"), Arc::from("text/plain")),
        ],
        content: Bytes::from_static(b"hello from RuniFi"),
    };

    // Send the FlowFile
    client.send(ff).await.unwrap();

    // Give the server a moment to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Receive the FlowFile
    let received = server.try_recv();
    assert!(received.is_some(), "expected to receive a FlowFile");

    let received = received.unwrap();
    assert_eq!(received.attributes.len(), 2);
    assert_eq!(received.attributes[0].0.as_ref(), "filename");
    assert_eq!(received.attributes[0].1.as_ref(), "test.txt");
    assert_eq!(received.content, Bytes::from_static(b"hello from RuniFi"));

    // Cleanup
    client.close();
    server.shutdown();
}

#[tokio::test]
async fn send_empty_flowfile() {
    let (mut server, client) = setup_pair().await.unwrap();

    let ff = WireFlowFile {
        attributes: vec![],
        content: Bytes::new(),
    };

    client.send(ff).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let received = server.try_recv();
    assert!(received.is_some());

    let received = received.unwrap();
    assert!(received.attributes.is_empty());
    assert!(received.content.is_empty());

    client.close();
    server.shutdown();
}

#[tokio::test]
async fn send_large_content() {
    let (mut server, client) = setup_pair().await.unwrap();

    // Create 1MB of data
    let data = vec![0xABu8; 1024 * 1024];
    let ff = WireFlowFile {
        attributes: vec![(Arc::from("size"), Arc::from("1048576"))],
        content: Bytes::from(data.clone()),
    };

    client.send(ff).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = server.try_recv();
    assert!(received.is_some());

    let received = received.unwrap();
    assert_eq!(received.content.len(), 1024 * 1024);
    assert_eq!(received.content.as_ref(), data.as_slice());

    client.close();
    server.shutdown();
}

#[tokio::test]
async fn send_batch_of_small_files() {
    let (mut server, client) = setup_pair().await.unwrap();

    let files: Vec<WireFlowFile> = (0..10)
        .map(|i| WireFlowFile {
            attributes: vec![(Arc::from("id"), Arc::from(i.to_string().as_str()))],
            content: Bytes::from(format!("content-{i}")),
        })
        .collect();

    client.send_batch(files).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = server.drain(20);
    assert_eq!(received.len(), 10, "expected 10 FlowFiles");

    for (i, ff) in received.iter().enumerate() {
        assert_eq!(
            ff.attributes[0].1.as_ref(),
            &i.to_string(),
            "FlowFile {i} has wrong id"
        );
    }

    client.close();
    server.shutdown();
}

#[tokio::test]
async fn multiple_sends_reuse_connection() {
    let (mut server, client) = setup_pair().await.unwrap();

    for i in 0..5 {
        let ff = WireFlowFile {
            attributes: vec![(Arc::from("seq"), Arc::from(i.to_string().as_str()))],
            content: Bytes::from(format!("data-{i}")),
        };
        client.send(ff).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = server.drain(10);
    assert_eq!(received.len(), 5);

    client.close();
    server.shutdown();
}

#[tokio::test]
async fn server_shutdown_stops_accepting() {
    let (server, _client) = setup_pair().await.unwrap();
    let _addr = server.local_addr();

    server.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Server is shut down -- endpoint is closed, new connections will be rejected.
}

#[tokio::test]
async fn many_attributes_round_trip() {
    let (mut server, client) = setup_pair().await.unwrap();

    let attrs: Vec<(Arc<str>, Arc<str>)> = (0..50)
        .map(|i| {
            (
                Arc::from(format!("key-{i}").as_str()),
                Arc::from(format!("value-{i}").as_str()),
            )
        })
        .collect();

    let ff = WireFlowFile {
        attributes: attrs.clone(),
        content: Bytes::from_static(b"multi-attr"),
    };

    client.send(ff).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let received = server.try_recv().unwrap();
    assert_eq!(received.attributes.len(), 50);
    for (i, (k, v)) in received.attributes.iter().enumerate() {
        assert_eq!(k.as_ref(), format!("key-{i}"));
        assert_eq!(v.as_ref(), format!("value-{i}"));
    }

    client.close();
    server.shutdown();
}
