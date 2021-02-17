use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::io::{Cursor, Result, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{copy, AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite};
use tokio_util::io::{ReaderStream, StreamReader};

/// Returns a BufferedStream backend by a temporary file.
///
/// The temporary file will be deleted when the result stream
/// is dropped.
pub async fn tempfile_buffered_stream<S>(bytes: S) -> Result<BufferedStream<File>>
where
    S: Stream<Item = Result<Bytes>> + Send + Sync,
{
    let tmp = File::from_std(tempfile()?);
    BufferedStream::new(tmp, bytes).await
}

/// Returns a BufferedStream backend by a in-memory buffer.
pub async fn memory_buffered_stream<S>(bytes: S) -> Result<BufferedStream<Cursor<Vec<u8>>>>
where
    S: Stream<Item = Result<Bytes>> + Send + Sync,
{
    BufferedStream::new(Cursor::new(Vec::new()), bytes).await
}

// A stream fully buffered by a backing store..
pub struct BufferedStream<R>
where
    R: AsyncRead + AsyncWrite + AsyncSeek + Unpin,
{
    size: usize,
    inner: ReaderStream<R>,
}

impl<R> BufferedStream<R>
where
    R: AsyncRead + AsyncWrite + AsyncSeek + Unpin,
{
    /// Consumes the bytes stream fully and writes its content into file.
    /// It returns a Stream implementation that reads the same content from the
    /// buffered file.
    ///
    /// The granularity of stream "chunks" will not be preserved.
    pub async fn new<S>(mut file: R, bytes: S) -> Result<BufferedStream<R>>
    where
        S: Stream<Item = Result<Bytes>> + Send + Sync,
    {
        let mut read = StreamReader::new(Box::pin(bytes));
        copy(&mut read, &mut file).await?;

        let size = file.seek(SeekFrom::End(0)).await? as usize;
        file.seek(SeekFrom::Start(0)).await?;

        Ok(Self {
            size,
            inner: ReaderStream::new(file),
        })
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl<R> Stream for BufferedStream<R>
where
    R: AsyncRead + AsyncWrite + AsyncSeek + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use futures::stream;
    use futures_test::stream::StreamTestExt;

    #[tokio::test]
    async fn test_buffered_stream() -> Result<()> {
        let stream = stream::iter(vec!["foo", "bar", "baz"])
            .map(|i| Ok(Bytes::from(i)))
            .interleave_pending();

        let buffer = std::io::Cursor::new(Vec::new()); // in-memory buffer
        let buf_stream = BufferedStream::new(buffer, stream).await?;
        assert_eq!(buf_stream.size(), 9);
        assert_eq!(buf_stream.size_hint(), (9, Some(9)));

        let mut all = BytesMut::new();
        for i in buf_stream.collect::<Vec<_>>().await {
            all.extend_from_slice(&i?);
        }

        assert_eq!(all, "foobarbaz");
        Ok(())
    }

    #[tokio::test]
    async fn test_tempfile_buffered_stream() -> Result<()> {
        let stream = stream::iter(vec!["foo", "bar", "baz"])
            .map(|i| Ok(Bytes::from(i)))
            .interleave_pending();

        let buf_stream = tempfile_buffered_stream(stream).await?;
        assert_eq!(buf_stream.size(), 9);
        assert_eq!(buf_stream.size_hint(), (9, Some(9)));

        let mut all = BytesMut::new();
        for i in buf_stream.collect::<Vec<_>>().await {
            all.extend_from_slice(&i?);
        }

        assert_eq!(all, "foobarbaz");
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_buffered_stream() -> Result<()> {
        let stream = stream::iter(vec!["foo", "bar", "baz"])
            .map(|i| Ok(Bytes::from(i)))
            .interleave_pending();

        let buf_stream = memory_buffered_stream(stream).await?;
        assert_eq!(buf_stream.size(), 9);
        assert_eq!(buf_stream.size_hint(), (9, Some(9)));

        let mut all = BytesMut::new();
        for i in buf_stream.collect::<Vec<_>>().await {
            all.extend_from_slice(&i?);
        }

        assert_eq!(all, "foobarbaz");
        Ok(())
    }
}
