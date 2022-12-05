use futures::{stream::Fuse, Stream, StreamExt};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub struct BatchStream<T, S> {
    container: Vec<T>,
    batch_size: usize,
    stream: Fuse<S>,
}

impl<T, S> BatchStream<T, S>
where
    S: Stream<Item = T> + Unpin,
{
    /// `batch_size` must greater than 0
    pub fn new(stream: S, batch_size: usize) -> Self {
        debug_assert!(batch_size > 0);
        Self {
            container: Vec::with_capacity(batch_size),
            batch_size,
            stream: stream.fuse(),
        }
    }
}

impl<T, S> Stream for BatchStream<T, S>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin,
{
    type Item = Vec<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        while let Poll::Ready(ele) = Pin::new(&mut this.stream).poll_next(cx) {
            match ele {
                Some(ele) => {
                    this.container.push(ele);
                    if this.container.len() == this.batch_size {
                        let mut res = Vec::with_capacity(this.batch_size);
                        std::mem::swap(&mut res, &mut this.container);
                        debug_assert_eq!(this.container.len(), 0);
                        return Poll::Ready(Some(res));
                    }
                }
                None => {
                    if this.container.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        let mut res = Vec::with_capacity(this.batch_size);
                        std::mem::swap(&mut res, &mut this.container);
                        debug_assert_eq!(this.container.len(), 0);
                        return Poll::Ready(Some(res));
                    }
                }
            }
        }
        Poll::Pending
    }
}

#[pin_project]
pub struct Interleave<I, J> {
    #[pin]
    a: Fuse<I>,
    #[pin]
    b: Fuse<J>,
    flag: bool,
}

pub fn interleave<I, J>(i: I, j: J) -> Interleave<I, J>
where
    I: Stream,
    J: Stream<Item = I::Item>,
{
    Interleave {
        a: i.fuse(),
        b: j.fuse(),
        flag: false,
    }
}

impl<I, J> Stream for Interleave<I, J>
where
    I: Stream,
    J: Stream<Item = I::Item>,
{
    type Item = I::Item;
    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let projection = self.project();
        *projection.flag = !*projection.flag;
        if *projection.flag {
            match projection.a.poll_next(cx) {
                Poll::Ready(None) => projection.b.poll_next(cx),
                r => r,
            }
        } else {
            match projection.b.poll_next(cx) {
                Poll::Ready(None) => projection.a.poll_next(cx),
                r => r,
            }
        }
    }
}
