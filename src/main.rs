#![allow(unused_imports)]
use bytes::Bytes;
use futures::{future, Stream, StreamExt};
use ru2::buffer::tempfile_buffered_stream;
use std::io::Result;
use std::io::SeekFrom;
use std::time::Duration;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::io::{ReaderStream, StreamReader};

#[allow(dead_code)]
fn get_stream(start: i64, end: i64) -> impl Stream<Item = Result<Bytes>> {
    IntervalStream::new(tokio::time::interval(Duration::from_millis(5))).scan(
        start,
        move |acc, _| {
            *acc += 1;
            match *acc {
                x if x == end => future::ready(None),
                _ => future::ready(Some(Ok(Bytes::from(format!("{},", *acc))))),
            }
        },
    )
}

fn main() -> Result<()> {
    let tokio_runtime = tokio::runtime::Runtime::new()?;
    tokio_runtime.block_on(amain())?;

    Ok(())
}

async fn amain() -> Result<()> {
    let stream = tempfile_buffered_stream(get_stream(10, 101)).await?;
    put(stream).await
}

pub async fn put<'a, S>(stream: S) -> Result<()>
where
    // This uses the same signature as resoto-s3's API.
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    let mut file = File::create("/tmp/foo.txt").await?;
    let mut read = StreamReader::new(Box::pin(stream));
    tokio::io::copy(&mut read, &mut file).await?;
    Ok(())
}
