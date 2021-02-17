use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::io::Result;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{copy, AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite};
use tokio_util::io::{ReaderStream, StreamReader};

// A stream fully buffered by a backing file.
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
        S: Stream<Item = Result<Bytes>> + Send + Sync + Unpin,
    {
        let mut read = StreamReader::new(bytes);
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
        (self.size, Some(self.size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use futures::stream;
    use futures_test::stream::StreamTestExt;
    use tempfile::tempfile;
    use tokio::fs::File;

    #[tokio::test]
    async fn test_buffered_stream() -> Result<()> {
        let stream = stream::iter(vec!["foo", "bar", "baz"])
            .map(|i| Ok(Bytes::from(i)))
            .interleave_pending();

        let mut file = File::from_std(tempfile()?);
        let buf_stream = BufferedStream::new(&mut file, stream).await?;
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
