use super::*;
use crate::nbd::{NbdServer, serve_connection};
use crate::{ChunkStore, Manifest};
use object_store::memory::InMemory;
use tempfile::TempDir;

async fn device() -> (TempDir, Arc<VolumeDevice>) {
    let dir = tempfile::tempdir().unwrap();
    let device = VolumeDevice::open(
        ChunkStore::new(Arc::new(InMemory::new())),
        Manifest::empty(4 * u64::from(swarmy_core::CHUNK_SIZE)).unwrap(),
        dir.path().join("cache"),
        dir.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    (dir, device)
}

async fn option(client: &mut UnixStream, number: u32, payload: &[u8]) {
    client.write_u64(OPTION_MAGIC).await.unwrap();
    client.write_u32(number).await.unwrap();
    client
        .write_u32(u32::try_from(payload.len()).unwrap())
        .await
        .unwrap();
    client.write_all(payload).await.unwrap();
}

async fn reply(client: &mut UnixStream, number: u32, kind: u32) -> Vec<u8> {
    assert_eq!(client.read_u64().await.unwrap(), OPTION_REPLY);
    assert_eq!(client.read_u32().await.unwrap(), number);
    assert_eq!(client.read_u32().await.unwrap(), kind);
    let length = client.read_u32().await.unwrap();
    let mut data = vec![0; length as usize];
    client.read_exact(&mut data).await.unwrap();
    data
}

async fn greeting(client: &mut UnixStream, flags: u32) {
    assert_eq!(client.read_u64().await.unwrap(), NBD_MAGIC);
    assert_eq!(client.read_u64().await.unwrap(), OPTION_MAGIC);
    assert_eq!(client.read_u16().await.unwrap(), 3);
    client.write_u32(flags).await.unwrap();
}

async fn request(
    client: &mut UnixStream,
    command: u16,
    flags: u16,
    offset: u64,
    length: u32,
    payload: &[u8],
    errno: u32,
) -> Vec<u8> {
    client.write_u32(REQUEST_MAGIC).await.unwrap();
    client.write_u16(flags).await.unwrap();
    client.write_u16(command).await.unwrap();
    client.write_u64(0xfedc_ba98_7654_3210).await.unwrap();
    client.write_u64(offset).await.unwrap();
    client.write_u32(length).await.unwrap();
    client.write_all(payload).await.unwrap();
    if command == 2 {
        return Vec::new();
    }
    assert_eq!(client.read_u32().await.unwrap(), REPLY_MAGIC);
    assert_eq!(client.read_u32().await.unwrap(), errno);
    assert_eq!(client.read_u64().await.unwrap(), 0xfedc_ba98_7654_3210);
    let mut output = vec![
        0;
        if command == 0 && errno == 0 {
            length as usize
        } else {
            0
        }
    ];
    client.read_exact(&mut output).await.unwrap();
    output
}

#[tokio::test]
async fn go_and_commands_over_listener() {
    let (dir, device) = device().await;
    let path = dir.path().join("nbd.sock");
    let server = NbdServer::bind(&path, Arc::clone(&device)).unwrap();
    let task = tokio::spawn(async move { server.run().await });
    let mut client = UnixStream::connect(&path).await.unwrap();
    greeting(&mut client, 3).await;
    option(&mut client, 99, b"unknown").await;
    reply(&mut client, 99, ERR_UNSUP).await;
    option(&mut client, 7, &[0]).await;
    reply(&mut client, 7, ERR_INVALID).await;
    option(&mut client, 7, &[0, 0, 0, 0, 0, 0]).await;
    reply(&mut client, 7, 3).await;
    reply(&mut client, 7, 3).await;
    reply(&mut client, 7, ERR_BLOCK_SIZE).await;
    option(&mut client, 7, &[0, 0, 0, 0, 0, 1, 0, 3]).await;
    let export = reply(&mut client, 7, 3).await;
    assert_eq!(&export[2..10], &device.size().to_be_bytes());
    assert_eq!(&export[10..], &EXPORT_FLAGS.to_be_bytes());
    let block = reply(&mut client, 7, 3).await;
    assert_eq!(&block[..6], &[0, 3, 0, 0, 16, 0]);
    reply(&mut client, 7, 1).await;
    assert_eq!(
        request(&mut client, 0, 0, 0, 4096, &[], 0).await,
        vec![0; 4096]
    );
    // Cross a chunk boundary, then trim only one of the dirty blocks.
    let offset = u64::from(swarmy_core::CHUNK_SIZE) - 4096;
    request(&mut client, 1, 0, offset, 8192, &vec![37; 8192], 0).await;
    assert_eq!(
        request(&mut client, 0, 0, offset, 8192, &[], 0).await,
        vec![37; 8192]
    );
    request(&mut client, 3, 0, 0, 0, &[], 0).await;
    request(&mut client, 4, 0, offset, 4096, &[], 0).await;
    assert_eq!(
        request(&mut client, 0, 0, offset, 4096, &[], 0).await,
        vec![0; 4096]
    );
    assert_eq!(
        request(&mut client, 0, 0, offset + 4096, 4096, &[], 0).await,
        vec![37; 4096]
    );
    request(&mut client, 1, 0, device.size(), 4096, &vec![1; 4096], 22).await;
    request(&mut client, 0, 0, u64::MAX - 4095, 8192, &[], 22).await;
    request(&mut client, 0, 0, 1, 4096, &[], 22).await;
    request(&mut client, 1, 1, 0, 4096, &vec![1; 4096], 22).await;
    request(&mut client, 3, 0, 0, 4096, &[], 22).await;
    request(&mut client, 42, 0, 0, 0, &[], 22).await;
    assert_eq!(
        request(&mut client, 0, 0, 0, 4096, &[], 0).await,
        vec![0; 4096]
    );
    request(&mut client, 2, 0, 0, 0, &[], 0).await;
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    task.abort();
    let _ = task.await;
    assert!(!path.exists());
}

#[tokio::test]
async fn export_name_padding_abort_and_bad_clients() {
    let (_dir, device) = device().await;
    for flags in [1, 3] {
        let (mut client, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(serve_connection(server, Arc::clone(&device)));
        greeting(&mut client, flags).await;
        option(&mut client, 1, &[]).await;
        assert_eq!(client.read_u64().await.unwrap(), device.size());
        assert_eq!(client.read_u16().await.unwrap(), EXPORT_FLAGS);
        if flags == 1 {
            let mut padding = [1; 124];
            client.read_exact(&mut padding).await.unwrap();
            assert_eq!(padding, [0; 124]);
        }
        request(&mut client, 2, 0, 0, 0, &[], 0).await;
        task.await.unwrap().unwrap();
    }
    let (mut client, server) = UnixStream::pair().unwrap();
    let task = tokio::spawn(serve_connection(server, Arc::clone(&device)));
    greeting(&mut client, 3).await;
    option(&mut client, 2, &[]).await;
    reply(&mut client, 2, 1).await;
    task.await.unwrap().unwrap();
    let (mut client, server) = UnixStream::pair().unwrap();
    let task = tokio::spawn(serve_connection(server, Arc::clone(&device)));
    greeting(&mut client, 7).await;
    assert!(task.await.unwrap().is_err());
    let (mut client, server) = UnixStream::pair().unwrap();
    let task = tokio::spawn(serve_connection(server, Arc::clone(&device)));
    negotiate_kernel(&mut client, device.size()).await.unwrap();
    client.write_u32(REQUEST_MAGIC).await.unwrap();
    client.write_u32(1).await.unwrap();
    client.write_u64(1).await.unwrap();
    client.write_u64(0).await.unwrap();
    client.write_u32(u32::MAX).await.unwrap();
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn missing_object_becomes_io_error_and_connection_survives() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let mut builder = crate::ManifestBuilder::new(
        store.clone(),
        Manifest::empty(u64::from(swarmy_core::CHUNK_SIZE)).unwrap(),
    );
    builder
        .set_chunk(0, swarmy_core::ContentHash([1; 32]))
        .unwrap();
    let manifest = builder.build().await.unwrap();
    let device = VolumeDevice::open(
        ChunkStore::new(store),
        manifest,
        dir.path().join("cache"),
        dir.path().join("dirty"),
        0,
    )
    .await
    .unwrap();
    let (mut client, server) = UnixStream::pair().unwrap();
    let task = tokio::spawn(serve_connection(server, Arc::clone(&device)));
    negotiate_kernel(&mut client, device.size()).await.unwrap();
    request(&mut client, 0, 0, 0, 4096, &[], 5).await;
    request(&mut client, 1, 0, 0, 4096, &vec![7; 4096], 0).await;
    assert_eq!(
        request(&mut client, 0, 0, 0, 4096, &[], 0).await,
        vec![7; 4096]
    );
    request(&mut client, 2, 0, 0, 0, &[], 0).await;
    task.await.unwrap().unwrap();
}
