#![allow(unused_imports)]
use bytes::Bytes;
use futures::{future, Stream, StreamExt};
use std::io::SeekFrom;
use std::time::Duration;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::io::{ReaderStream, StreamReader};

use ru2::buffer::BufferedStream;

#[allow(dead_code)]
fn get_stream(start: i64, end: i64) -> impl Stream<Item = std::io::Result<Bytes>> {
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

fn main() -> std::io::Result<()> {
    let tokio_runtime = tokio::runtime::Runtime::new()?;
    tokio_runtime.block_on(amain())?;

    Ok(())
}

async fn amain() -> std::io::Result<()> {
    let stream = get_stream(10, 100);
    let tmp = File::from_std(tempfile()?);
    //let stream = tokio_util::io::ReaderStream::new(tmp);
    let stream = BufferedStream::new(tmp, stream).await?;
    put(stream);

    Ok(())
}

pub fn put<'a, S>(_stream: S)
where
    // This uses the same signature as resoto-s3's API.
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    println!("TODO");
}
