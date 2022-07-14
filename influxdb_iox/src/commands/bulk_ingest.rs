use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use itertools::Itertools;
use object_store::DynObjectStore;
use object_store::memory::InMemory;
use snafu::{ResultExt, Snafu};
use tokio::time::sleep;
use data_types::{KafkaPartition, Sequence, SequenceNumber};
use iox_catalog::interface::Catalog;
use iox_catalog::mem::MemCatalog;
use iox_query::exec::Executor;
use mutable_batch_lp;
use data_types::NamespaceSchema;
use iox_catalog::validate_or_insert_schema;
use iox_time::{SystemProvider, Time};
use backoff::{BackoffConfig};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use ingester::data::{IngesterData, SequencerData};
use ingester::lifecycle::{LifecycleConfig, LifecycleHandle, LifecycleManager};

/// Bulk ingest data without saving it in kafka
///
/// TODO: docs
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// File with data to load. Currently supported formats are .lp
    #[clap(action)]
    file_name: PathBuf,
}

#[derive(Debug, Snafu)]
pub enum Error {

    #[snafu(display("File io failed: {}", source))]
    FileIo {
        source: std::io::Error,
    },

    #[snafu(display("Converting LP to mutable batch failed: {}", source))]
    MutableBatch {
        source: mutable_batch_lp::Error,
    },

    #[snafu(display("Persisting mutable batch failed {}", source))]
    Ingester {
        source: ingester::data::Error,
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn command(config: Config) -> Result<()> {
    let file = File::open(config.file_name).context(FileIoSnafu)?;
    let file = BufReader::with_capacity(1000000, file);
    let lines = &mut file.lines().peekable();

    let metrics = Arc::new(metric::Registry::new());
    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
    let mut repos = catalog.repositories().await;
    let kafka_topic = repos.kafka_topics().create_or_get("whatevs").await.unwrap();
    let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
    let kafka_partition = KafkaPartition::new(0);
    let namespace = repos
        .namespaces()
        .create("0000000000000001_0000000000000002", "inf", kafka_topic.id, query_pool.id)
        .await
        .unwrap();
    let sequencer1 = repos
        .sequencers()
        .create_or_get(&kafka_topic, kafka_partition)
        .await
        .unwrap();
    let mut sequencers = BTreeMap::new();
    sequencers.insert(
        sequencer1.id,
        SequencerData::new(sequencer1.kafka_partition, Arc::clone(&metrics)),
    );
    let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());
    let executor = Arc::new(Executor::new(1));
    let data = Arc::new(IngesterData::new(
        Arc::clone(&object_store),
        Arc::clone(&catalog),
        sequencers,
        executor.clone(),
        BackoffConfig::default(),
    ));
    let schema = NamespaceSchema::new(namespace.id, kafka_topic.id, query_pool.id);

    let ignored_ts = Time::from_timestamp_millis(42);

    let manager = LifecycleManager::new(
        LifecycleConfig::new(1, 0, 0, Duration::from_secs(1), Duration::from_secs(1)),
        metrics,
        Arc::new(SystemProvider::new()),
    );
    drop(repos);

    loop {
        let mut err = Ok(());

        let lp = lines.take(100000).filter_map(|x| match x {
            Ok(s) => Some(s),
            Err(e) => {err = Err(e); None}
        }).join("\n");
        err.context(FileIoSnafu)?;
        if lp.is_empty() {
            break;
        }
        let start = Instant::now();
        println!("creating batch");
        let batches = mutable_batch_lp::lines_to_batches(&lp, 0).context(MutableBatchSnafu)?;
        let end = Instant::now();
        println!("created batch duration {}s", (end-start).as_secs());
        let start = end;
        let w1 = DmlWrite::new(
            "0000000000000001_0000000000000002",
            batches,
            Some("1970-01-01".into()), //FIXME: bad partition key
            DmlMeta::sequenced(
                Sequence::new(1, SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        validate_or_insert_schema(w1.tables(), &schema, &mut *catalog.repositories().await)
            .await
            .expect("Schema failed validation");
        let end = Instant::now();
        println!("validated schema duration {}s", (end-start).as_secs());
        let start = end;

        let mut should_pause = data.buffer_operation(sequencer1.id, DmlOperation::Write(w1), &manager.handle()).await.context(IngesterSnafu)?;
        if should_pause {
            println!("Paused buffer ingest!")
        }
        while should_pause {
            sleep(Duration::from_millis(100)).await;
            should_pause = manager.handle().can_resume_ingest();
        }

        let end = Instant::now();
        println!("ingested duration {}s", (end-start).as_secs());
    }
    executor.join().await;
    Ok(())
}
