use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use itertools::Itertools;
use rand::Rng;
use snafu::{ResultExt, Snafu};
use tokio::time::sleep;
use data_types::{DatabaseName, KafkaPartition, Namespace, PartitionTemplate, TemplatePart};
use iox_query::exec::Executor;
use mutable_batch_lp;
use data_types::NamespaceSchema;
use iox_catalog::validate_or_insert_schema;
use iox_time::{SystemProvider, Time};
use backoff::{BackoffConfig};
use clap_blocks::catalog_dsn::CatalogDsnConfig;
use clap_blocks::object_store::{make_object_store, ObjectStoreConfig};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use ingester::data::{IngesterData, Persister, SequencerData};
use ingester::lifecycle::{LifecycleConfig, LifecycleManager};
use iox_catalog::interface::Catalog;
use router::dml_handlers::{DmlHandler, Partitioner};

/// Bulk ingest data without saving it in kafka
///
/// TODO: docs
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// File with data to load. Currently supported formats are .lp
    #[clap(action)]
    file_name: PathBuf,

    #[clap(flatten)]
    object_store: ObjectStoreConfig,

    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("File io failed: {}", source))]
    FileIo {
        source: std::io::Error,
    },

    #[snafu(display("Catalog read failed: {}", source))]
    CatalogRead {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Converting LP to mutable batch failed: {}", source))]
    MutableBatch {
        source: mutable_batch_lp::Error,
    },

    #[snafu(display("Persisting mutable batch failed: {}", source))]
    Ingester {
        source: ingester::data::Error,
    },

    #[snafu(display("Partitioning write batch failed: {}", source))]
    Partitioner {
        source: router::dml_handlers::PartitionError,
    },

    #[snafu(display("Creating object store from config failed: {}", source))]
    ObjectStoreParse {
        source: clap_blocks::object_store::ParseError,
    },

    #[snafu(display("Creating catalog from config failed: {}", source))]
    CatalogParse {
        source: clap_blocks::catalog_dsn::Error,
    },

}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn persist_all(catalog : &dyn Catalog, namespace : &Namespace, data: &IngesterData) -> Result<()> {
    let start = Instant::now();
    println!("persisting partitions");
    let mut repos = catalog.repositories().await;
    let partitions = repos.partitions().list_by_namespace(namespace.id).await.context(CatalogReadSnafu)?;
    drop(repos);
    for partition in partitions {
        data.persist(partition.id).await;
    }
    let end = Instant::now();
    println!("persisted partitions {}s", (end - start).as_secs());
    Ok(())
}

pub async fn command(config: Config) -> Result<()> {
    let mut stdin_lines;
    let mut buf_lines;
    let lines : &mut dyn Iterator<Item=_> = if config.file_name.to_string_lossy() == "-" {
        stdin_lines = io::stdin().lock().lines();
        &mut stdin_lines
    } else {
        let file = File::open(config.file_name).context(FileIoSnafu)?;
        buf_lines = BufReader::with_capacity(10000000, file).lines();
        &mut buf_lines
    };

    let metrics = Arc::new(metric::Registry::new());
    let object_store = make_object_store(&config.object_store).context(ObjectStoreParseSnafu)?;
    let catalog = config.catalog_dsn
        .get_catalog("iox_objectstore_garbage_collect", metrics.clone())
        .await.context(CatalogParseSnafu)?;
    println!("using catalog: {:?}", config.catalog_dsn);
    println!("using object store: {:?}", config.object_store);

    let namespace_name = "0000000000000001_0000000000000002";

    let kafka_topic;
    let query_pool;
    let namespace;
    {
        let mut transaction = catalog.start_transaction().await.unwrap();
        kafka_topic = transaction.kafka_topics().create_or_get("whatevs").await.unwrap();
        query_pool = transaction.query_pools().create_or_get("whatevs").await.unwrap();
        namespace = match transaction.namespaces().get_by_name(namespace_name).await.unwrap() {
            Some(n) => n,
            None => {
                transaction.namespaces()
                    .create(namespace_name, "inf", kafka_topic.id, query_pool.id)
                    .await
                    .unwrap()
            }
        };
        transaction.commit().await.unwrap();
    }

    let mut repos = catalog.repositories().await;

    let kafka_partition = KafkaPartition::new(0);

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

    let gb = 1000 * 1000 * 1000;
    let mut manager = LifecycleManager::new(
        LifecycleConfig::new(
            4*gb,
            2*gb,
            1*gb,
            Duration::from_secs(120),
            Duration::from_secs(10),
        ),
        metrics,
        Arc::new(SystemProvider::new()),
    );

    let partitioner = Partitioner::new(PartitionTemplate {
        parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
    });

    let database_name = DatabaseName::new("0000000000000001_0000000000000002").expect("valid database name");

    drop(repos);

    let lines_per_loop : i64 = 100000;
    let mut count : i64 = 0;
    loop {
        if count % 100 == 0 {
            println!("Finished {} lines", count*lines_per_loop)
        }
        count += 1;
        let mut err = Ok(());

        let lp = lines.take(lines_per_loop as usize).filter_map(|x| match x {
            Ok(s) => Some(s),
            Err(e) => {
                err = Err(e);
                None
            }
        }).join("\n");
        err.context(FileIoSnafu)?;
        if lp.is_empty() {
            break;
        }
        let batches = mutable_batch_lp::lines_to_batches(&lp, 0).context(MutableBatchSnafu)?;

        let partitioned = partitioner.write(&database_name.clone(), batches, None).await.context(PartitionerSnafu)?;

        let writes: Vec<_> = partitioned.into_iter().map(|p| {
            let (partition_key, partition_batches) = p.into_parts();
            DmlWrite::new(
                database_name.clone(),
                partition_batches,
                Some(partition_key),
                DmlMeta::timed(
                    ignored_ts,
                    None,
                    50,
                ),
            )
        }).collect();

        for write in &writes {
            loop {
                match validate_or_insert_schema(write.tables(), &schema, &mut *catalog.repositories().await)
                    .await {
                    Err(e) => {
                        let seconds = rand::thread_rng().gen_range(0..20);
                        println!("Retrying schema validation after {} seconds, error: {:?}", seconds, e);
                        sleep(Duration::from_secs(seconds)).await;
                    },
                    Ok(_) => break,
                }
            }
        }

        for write in writes {
            let should_pause = data.buffer_operation(sequencer1.id, DmlOperation::Write(write), &manager.handle()).await.context(IngesterSnafu)?;
            if should_pause {
                let start = Instant::now();
                manager.maybe_persist(&data).await;
                let end = Instant::now();
                println!("persisted partitions to dump memory {}s", (end - start).as_secs());
            }
        }
    }
    persist_all(&*catalog, &namespace, &data).await?;
    executor.join().await;
    Ok(())
}
