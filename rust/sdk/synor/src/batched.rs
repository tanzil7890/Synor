//! `Batched` — call a batch implementation on **single values**, with per-item
//! memoization and automatic coalescing of concurrent cache-misses into one
//! batch call (via the core batcher, `synor_utils::batching`).
//!
//! This is the single batching mechanism, composable with memoization: each
//! `call` memo-probes its own item; only misses execute, and concurrent misses
//! (even across different components) are combined into one invocation of the
//! batch implementation. You never assemble a list yourself.
//!
//! ```ignore
//! #[synor::function]                                   // ctx-free batch impl; emits a logic hash
//! async fn embed_batch(texts: Vec<String>) -> synor::Result<Vec<Vec<f32>>> {
//!     model.encode(&texts)
//! }
//!
//! static EMBED: std::sync::LazyLock<synor::Batched<String, Vec<f32>>> =
//!     std::sync::LazyLock::new(|| synor::Batched::new(embed_batch, __SYNOR_FN_HASH_EMBED_BATCH));
//!
//! // Call per single item (e.g. inside ctx.map over chunks):
//! let emb = EMBED.call(&ctx, text).await?;
//! ```

use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use synor_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};

use crate::ctx::Ctx;
use crate::error::{Error, Result};

type BatchFuture<Out> = Pin<Box<dyn Future<Output = synor_utils::error::Result<Vec<Out>>> + Send>>;
type BatchFn<In, Out> = Box<dyn Fn(Vec<In>) -> BatchFuture<Out> + Send + Sync>;
type InputSizeFn<In> = Box<dyn Fn(&In) -> usize + Send + Sync>;

/// Adapts a user closure `Vec<In> -> Result<Vec<Out>>` to the core batcher's `Runner`.
struct FnRunner<In, Out> {
    f: BatchFn<In, Out>,
    input_size: Option<InputSizeFn<In>>,
}

/// Counts serialized bytes without retaining a second copy of the input.
///
/// `rmp_serde::to_vec` is convenient but briefly doubles large payloads before
/// the batcher's byte semaphore can account for them. This writer gives the
/// serializer the same byte-counting signal with constant additional memory.
#[derive(Default)]
struct CountingWriter {
    bytes_written: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes_written = self.bytes_written.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_size_bytes<T: Serialize>(value: &T) -> Option<usize> {
    let mut writer = CountingWriter::default();
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut writer))
        .ok()?;
    Some(writer.bytes_written)
}

#[async_trait]
impl<In: Serialize + Send + 'static, Out: Send + 'static> Runner for FnRunner<In, Out> {
    type Input = In;
    type Output = Out;

    fn input_size_bytes(&self, input: &Self::Input) -> usize {
        // `In` is intentionally generic, so its serialized representation is
        // the most reliable available proxy for owned heap content (String,
        // Vec, nested structs, and similar values). Include the inline value
        // itself and fail conservatively to the prior shallow estimate if a
        // custom serializer rejects this particular value.
        let encoded_estimate = std::mem::size_of_val(input)
            .saturating_add(serialized_size_bytes(input).unwrap_or_default());
        self.input_size
            .as_ref()
            .map(|estimate| estimate(input))
            .unwrap_or_default()
            .max(encoded_estimate)
            .max(1)
    }

    async fn run(
        &self,
        inputs: Vec<In>,
    ) -> synor_utils::error::Result<impl ExactSizeIterator<Item = Out>> {
        let outputs = (self.f)(inputs).await?;
        Ok(outputs.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use synor_utils::batching::Runner;

    use super::{FnRunner, serialized_size_bytes};

    #[test]
    fn counting_serializer_matches_messagepack_output_length() {
        let value = vec!["a substantial payload".repeat(128), "tail".to_owned()];
        assert_eq!(
            serialized_size_bytes(&value),
            Some(rmp_serde::to_vec(&value).unwrap().len())
        );
    }

    #[test]
    fn explicit_estimator_accounts_for_retained_spare_capacity() {
        let runner: FnRunner<String, ()> = FnRunner {
            f: Box::new(|_| {
                Box::pin(async { Ok::<Vec<()>, synor_utils::error::Error>(Vec::new()) })
            }),
            input_size: Some(Box::new(|input| input.capacity())),
        };
        let mut input = String::with_capacity(16 * 1024);
        input.push('x');

        assert!(runner.input_size_bytes(&input) >= 16 * 1024);
    }
}

/// A batched, memoized function. See the [module docs](self).
pub struct Batched<In, Out>
where
    In: Serialize + Send + 'static,
    Out: Send + 'static,
{
    batcher: Arc<Batcher<FnRunner<In, Out>>>,
    code_hash: u64,
}

impl<In, Out> Batched<In, Out>
where
    In: Serialize + Send + 'static,
    Out: Serialize + DeserializeOwned + Send + 'static,
{
    /// Build a `Batched` from a batch implementation `f: Vec<In> -> Result<Vec<Out>>`.
    ///
    /// `code_hash` is the batch function's logic fingerprint — the
    /// `__SYNOR_FN_HASH_*` constant emitted by `#[synor::function]`. It is
    /// folded into each item's memo key, so editing the batch logic invalidates
    /// cached results.
    pub fn new<F, Fut>(f: F, code_hash: u64) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        Self::with_options(f, code_hash, BatchingOptions::default(), None)
    }

    /// Build a `Batched` with an explicit retained-memory estimator for inputs.
    ///
    /// Serialization provides a safe, allocation-free estimate of logical
    /// payload bytes, but it cannot observe spare `Vec`/`String` capacity,
    /// shared backing allocations, or memory hidden by a custom serializer.
    /// The callback should return total retained bytes for one input. Synor
    /// uses the larger of that value and its encoded estimate for admission.
    pub fn with_input_size_estimator<F, Fut, S>(f: F, code_hash: u64, input_size: S) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
        S: Fn(&In) -> usize + Send + Sync + 'static,
    {
        Self::with_options(
            f,
            code_hash,
            BatchingOptions::default(),
            Some(Box::new(input_size)),
        )
    }

    /// Like [`Batched::new`], but caps how many items are processed per batch.
    pub fn with_max_batch<F, Fut>(f: F, code_hash: u64, max_batch_size: usize) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        Self::with_options(
            f,
            code_hash,
            BatchingOptions {
                max_batch_size: Some(max_batch_size),
                ..BatchingOptions::default()
            },
            None,
        )
    }

    fn with_options<F, Fut>(
        f: F,
        code_hash: u64,
        options: BatchingOptions,
        input_size: Option<InputSizeFn<In>>,
    ) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        let wrapped: BatchFn<In, Out> = Box::new(move |inputs| {
            let fut = f(inputs);
            Box::pin(async move { fut.await.map_err(Error::into_core) })
        });
        let runner = FnRunner {
            f: wrapped,
            input_size,
        };
        let queue = Arc::new(BatchQueue::new());
        let batcher = Arc::new(Batcher::new(runner, queue, options));
        Self { batcher, code_hash }
    }

    /// Process one item. On a memo hit the stored result is returned; on a miss
    /// the item is handed to the core batcher (coalesced with other concurrent
    /// misses) and the result is memoized.
    pub async fn call(&self, ctx: &Ctx, item: In) -> Result<Out> {
        let fp = crate::memo::key_fingerprint_result(&("synor_batched", self.code_hash, &item))?;
        let batcher = self.batcher.clone();
        // A batch impl is ctx-free, so it makes no tracked child `#[function]`
        // calls; the `propagate_children_fn_logic` flag is therefore inert here.
        // Pass `true` (the default) — only the batch impl's own `code_hash`
        // (folded into the memo key above) tracks its logic.
        crate::memo::cached_by_fingerprint(ctx, fp, true, move |_ctx| async move {
            batcher.run(item).await.map_err(Error::from)
        })
        .await
    }
}
