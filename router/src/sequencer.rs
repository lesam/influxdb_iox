//! A representation of a single operation sequencer.

use std::{borrow::Cow, hash::Hash, sync::Arc};

use data_types::KafkaPartition;
use dml::{DmlMeta, DmlOperation};
use iox_time::{SystemProvider, TimeProvider};
use metric::{Metric, U64Histogram, U64HistogramOptions};
use write_buffer::core::{WriteBufferError, WriteBufferWriting};

/// A sequencer tags an write buffer with a sequencer ID.
#[derive(Debug)]
pub struct Sequencer<P = SystemProvider> {
    kafka_partition: KafkaPartition,
    inner: Arc<dyn WriteBufferWriting>,
    time_provider: P,

    enqueue_success: U64Histogram,
    enqueue_error: U64Histogram,
}

impl Eq for Sequencer {}

impl PartialEq for Sequencer {
    fn eq(&self, other: &Self) -> bool {
        self.kafka_partition == other.kafka_partition
    }
}

impl Hash for Sequencer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kafka_partition.hash(state);
    }
}

impl Sequencer {
    /// Tag `inner` with the specified `id`.
    pub fn new(
        kafka_partition: KafkaPartition,
        inner: Arc<dyn WriteBufferWriting>,
        metrics: &metric::Registry,
    ) -> Self {
        let buckets = || {
            U64HistogramOptions::new([5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, u64::MAX])
        };
        let write: Metric<U64Histogram> = metrics.register_metric_with_options(
            "sequencer_enqueue_duration_ms",
            "sequencer enqueue call duration in milliseconds",
            buckets,
        );

        let kafka_partition_string = kafka_partition.get().to_string();
        let enqueue_success = write.recorder([
            ("kafka_partition", Cow::from(kafka_partition_string.clone())),
            ("result", Cow::from("success")),
        ]);
        let enqueue_error = write.recorder([
            ("kafka_partition", Cow::from(kafka_partition_string)),
            ("result", Cow::from("error")),
        ]);

        Self {
            kafka_partition,
            inner,
            enqueue_success,
            enqueue_error,
            time_provider: Default::default(),
        }
    }

    /// Return the ID of this sequencer.
    pub fn kafka_partition(&self) -> KafkaPartition {
        self.kafka_partition
    }

    /// Enqueue `op` into this sequencer.
    ///
    /// The buffering / async return behaviour of this method is defined by the
    /// behaviour of the [`WriteBufferWriting::store_operation()`]
    /// implementation this [`Sequencer`] wraps.
    pub async fn enqueue<'a>(&self, op: DmlOperation) -> Result<DmlMeta, WriteBufferError> {
        let t = self.time_provider.now();

        let res = self.inner.store_operation(self.kafka_partition, &op).await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.enqueue_success.record(delta.as_millis() as _),
                Err(_) => self.enqueue_error.record(delta.as_millis() as _),
            }
        }

        res
    }
}
