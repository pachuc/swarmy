//! NBD wire encoding and validation, independent of Linux ioctls.
use crate::{VolumeDevice, VolumeError, device::MAX_REQUEST};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

const NBD_MAGIC: u64 = 0x4e42_444d_4147_4943;
const OPTION_MAGIC: u64 = 0x4948_4156_454f_5054;
const OPTION_REPLY: u64 = 0x0003_e889_0455_65a9;
pub(crate) const EXPORT_FLAGS: u16 = 1 | 4 | 32;
const REQUEST_MAGIC: u32 = 0x2560_9513;
const REPLY_MAGIC: u32 = 0x6744_6698;
const ERR_UNSUP: u32 = 0x8000_0001;
const ERR_INVALID: u32 = 0x8000_0003;
const ERR_UNKNOWN: u32 = 0x8000_0006;
const ERR_BLOCK_SIZE: u32 = 0x8000_0008;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

async fn option_reply(
    stream: &mut UnixStream,
    option: u32,
    kind: u32,
    data: &[u8],
) -> io::Result<()> {
    stream.write_u64(OPTION_REPLY).await?;
    stream.write_u32(option).await?;
    stream.write_u32(kind).await?;
    stream
        .write_u32(u32::try_from(data.len()).map_err(|_| invalid("option reply too large"))?)
        .await?;
    stream.write_all(data).await
}

pub(super) async fn handshake(stream: &mut UnixStream, size: u64) -> io::Result<bool> {
    stream.write_u64(NBD_MAGIC).await?;
    stream.write_u64(OPTION_MAGIC).await?;
    stream.write_u16(3).await?;
    let flags = stream.read_u32().await?;
    if flags & 1 == 0 || flags & !3 != 0 {
        return Err(invalid("invalid client flags"));
    }
    loop {
        if stream.read_u64().await? != OPTION_MAGIC {
            return Err(invalid("invalid option magic"));
        }
        let option = stream.read_u32().await?;
        let length = stream.read_u32().await? as usize;
        if length > 65536 {
            return Err(invalid("option too large"));
        }
        let mut data = vec![0; length];
        stream.read_exact(&mut data).await?;
        match option {
            1 => {
                if !data.is_empty() {
                    return Err(invalid("unknown export"));
                }
                stream.write_u64(size).await?;
                stream.write_u16(EXPORT_FLAGS).await?;
                if flags & 2 == 0 {
                    stream.write_all(&[0; 124]).await?;
                }
                return Ok(true);
            }
            2 if data.is_empty() => {
                option_reply(stream, option, 1, &[]).await?;
                return Ok(false);
            }
            2 => option_reply(stream, option, ERR_INVALID, &[]).await?,
            6 | 7 => {
                let Some((name, infos)) = parse_go(&data) else {
                    option_reply(stream, option, ERR_INVALID, &[]).await?;
                    continue;
                };
                if !name.is_empty() {
                    option_reply(stream, option, ERR_UNKNOWN, &[]).await?;
                    continue;
                }
                let mut export = vec![0, 0]; // NBD_INFO_EXPORT
                export.extend_from_slice(&size.to_be_bytes());
                export.extend_from_slice(&EXPORT_FLAGS.to_be_bytes());
                option_reply(stream, option, 3, &export).await?;
                let mut block = vec![0, 3]; // NBD_INFO_BLOCK_SIZE
                block.extend_from_slice(&4096_u32.to_be_bytes());
                block.extend_from_slice(&4096_u32.to_be_bytes());
                block.extend_from_slice(
                    &u32::try_from(MAX_REQUEST)
                        .expect("request limit fits u32")
                        .to_be_bytes(),
                );
                option_reply(stream, option, 3, &block).await?;
                if !infos.as_chunks::<2>().0.contains(&[0, 3]) {
                    option_reply(stream, option, ERR_BLOCK_SIZE, &[]).await?;
                    continue;
                }
                option_reply(stream, option, 1, &[]).await?;
                if option == 7 {
                    return Ok(true);
                }
            }
            _ => option_reply(stream, option, ERR_UNSUP, &[]).await?,
        }
    }
}

fn parse_go(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let length = u32::from_be_bytes(data.get(..4)?.try_into().ok()?) as usize;
    let name = data.get(4..4_usize.checked_add(length)?)?;
    let remaining = data.get(4 + length..)?;
    let count = u16::from_be_bytes(remaining.get(..2)?.try_into().ok()?) as usize;
    let infos = remaining.get(2..)?;
    (infos.len() == count * 2).then_some((name, infos))
}

pub(super) async fn transmission(
    stream: &mut UnixStream,
    device: &Arc<VolumeDevice>,
) -> io::Result<()> {
    loop {
        // EOF at a request boundary is a normal peer disconnect, but a truncated
        // header or payload is an error and cannot be resumed safely.
        let mut first = [0];
        if stream.read(&mut first).await? == 0 {
            return Ok(());
        }
        let mut rest = [0; 3];
        stream.read_exact(&mut rest).await?;
        if u32::from_be_bytes([first[0], rest[0], rest[1], rest[2]]) != REQUEST_MAGIC {
            return Err(invalid("invalid request magic"));
        }
        let flags = stream.read_u16().await?;
        let command = stream.read_u16().await?;
        let handle = stream.read_u64().await?;
        let offset = stream.read_u64().await?;
        let length = stream.read_u32().await? as usize;
        if command == 2 {
            return Ok(());
        }
        if command != 4 && length > MAX_REQUEST {
            return Err(invalid("request too large"));
        }
        // Consume even invalid writes so the following request stays aligned.
        let mut payload = vec![0; if command == 1 { length } else { 0 }];
        stream.read_exact(&mut payload).await?;
        let result = if flags != 0 {
            Err(VolumeError::InvalidRequest)
        } else {
            match command {
                0 => device.read(offset, length).await,
                1 => device.write(offset, &payload).await.map(|()| Vec::new()),
                3 if offset == 0 && length == 0 => device.flush().await.map(|()| Vec::new()),
                4 => device.trim(offset, length).await.map(|()| Vec::new()),
                _ => Err(VolumeError::InvalidRequest),
            }
        };
        let errno = match &result {
            Ok(_) => 0,
            Err(VolumeError::InvalidRequest) => 22,
            Err(error) => {
                tracing::warn!(%error, command, offset, length, "NBD request failed");
                5
            }
        };
        stream.write_u32(REPLY_MAGIC).await?;
        stream.write_u32(errno).await?;
        stream.write_u64(handle).await?;
        if let Ok(data) = result {
            stream.write_all(&data).await?;
        }
    }
}

pub(crate) async fn negotiate_kernel(stream: &mut UnixStream, size: u64) -> io::Result<()> {
    if stream.read_u64().await? != NBD_MAGIC
        || stream.read_u64().await? != OPTION_MAGIC
        || stream.read_u16().await? != 3
    {
        return Err(invalid("unexpected server handshake"));
    }
    stream.write_u32(3).await?;
    stream.write_u64(OPTION_MAGIC).await?;
    stream.write_u32(1).await?;
    stream.write_u32(0).await?;
    if stream.read_u64().await? != size || stream.read_u16().await? != EXPORT_FLAGS {
        return Err(invalid("unexpected export"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
