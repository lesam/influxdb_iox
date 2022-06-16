use self::{
    flight_client::{Error as FlightClientError, FlightClient, FlightClientImpl, FlightError},
    test_util::MockIngesterConnection,
};
use crate::cache::CatalogCache;
use arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};
use async_trait::async_trait;
use client_util::connection;
use data_types::{
    ChunkId, ChunkOrder, ColumnSummary, InfluxDbType, PartitionId, SequenceNumber, SequencerId,
    StatValues, Statistics, TableSummary, TimestampMinMax,
};
use datafusion::physical_plan::memory::MemoryStream;
use futures::{stream::FuturesUnordered, TryStreamExt};
use generated_types::{
    influxdata::iox::ingester::v1::GetWriteInfoResponse,
    ingester::{encode_proto_predicate_as_base64, IngesterQueryRequest},
    write_info::merge_responses,
};
use influxdb_iox_client::flight::low_level::LowLevelMessage;
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    util::compute_timenanosecond_min_max,
    QueryChunk, QueryChunkError, QueryChunkMeta,
};
use iox_time::{Time, TimeProvider};
use metric::{DurationHistogram, Metric};
use observability_deps::tracing::{debug, info, trace, warn};
use predicate::{Predicate, PredicateMatch};
use schema::{selection::Selection, sort::SortKey, InfluxColumnType, InfluxFieldType, Schema};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{any::Any, collections::HashMap, sync::Arc};

pub(crate) mod flight_client;
pub(crate) mod test_util;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "Internal error: \
        ingester record batch for column '{}' has type '{}' but should have type '{}'",
        column_name,
        actual_data_type,
        desired_data_type
    ))]
    RecordBatchType {
        column_name: String,
        actual_data_type: DataType,
        desired_data_type: DataType,
    },

    #[snafu(display(
        "Internal error: \
        failed to resolve ingester record batch types for column '{}' type '{}': {}",
        column_name,
        data_type,
        source
    ))]
    ConvertingRecordBatch {
        column_name: String,
        data_type: DataType,
        source: ArrowError,
    },

    #[snafu(display("Internal error creating record batch: {}", source))]
    CreatingRecordBatch { source: ArrowError },

    #[snafu(display("Failed ingester query '{}': {}", ingester_address, source))]
    RemoteQuery {
        ingester_address: String,
        source: FlightClientError,
    },

    #[snafu(display("Failed to connect to ingester '{}': {}", ingester_address, source))]
    Connecting {
        ingester_address: String,
        source: connection::Error,
    },

    #[snafu(display(
        "Error retrieving write info from '{}' for write token '{}': {}",
        ingester_address,
        write_token,
        source,
    ))]
    WriteInfo {
        ingester_address: String,
        write_token: String,
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display(
        "Partition status missing for partition {partition_id}, ingestger: {ingester_address}"
    ))]
    PartitionStatusMissing {
        partition_id: i64,
        ingester_address: String,
    },

    #[snafu(display(
        "Got batch without partition information from ingestger: {ingester_address}"
    ))]
    BatchWithoutPartition { ingester_address: String },

    #[snafu(display(
        "Duplicate partition info for partition {partition_id}, ingestger: {ingester_address}"
    ))]
    DuplicatePartitionInfo {
        partition_id: i64,
        ingester_address: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Create a new connection given Vec of `ingester_address` such as
/// "http://127.0.0.1:8083"
pub fn create_ingester_connection(
    ingester_addresses: Vec<String>,
    catalog_cache: Arc<CatalogCache>,
) -> Arc<dyn IngesterConnection> {
    Arc::new(IngesterConnectionImpl::new(
        ingester_addresses,
        catalog_cache,
    ))
}

/// Create a new ingester suitable for testing
pub fn create_ingester_connection_for_testing() -> Arc<dyn IngesterConnection> {
    Arc::new(MockIngesterConnection::new())
}

/// Handles communicating with the ingester(s) to retrieve
/// data that is not yet persisted
#[async_trait]
pub trait IngesterConnection: std::fmt::Debug + Send + Sync + 'static {
    /// Returns all partitions ingester(s) know about for the specified table.
    async fn partitions(
        &self,
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &Predicate,
        expected_schema: Arc<Schema>,
    ) -> Result<Vec<IngesterPartition>>;

    /// Returns the most recent partition sstatus info across all ingester(s) for the specified
    /// write token.
    async fn get_write_info(&self, write_token: &str) -> Result<GetWriteInfoResponse>;

    /// Return backend as [`Any`] which can be used to downcast to a specifc implementation.
    fn as_any(&self) -> &dyn Any;
}

/// Structure that holds metrics for ingester connections.
#[derive(Debug)]
struct IngesterConnectionMetrics {
    /// Time spent waiting for successful ingester queries
    ingester_duration_success: DurationHistogram,

    /// Time spent waiting for unsuccessful ingester queries
    ingester_duration_error: DurationHistogram,

    /// Time spent waiting for a request that was cancelled.
    ingester_duration_cancelled: DurationHistogram,
}

impl IngesterConnectionMetrics {
    fn new(metric_registry: &metric::Registry) -> Self {
        let ingester_duration: Metric<DurationHistogram> = metric_registry.register_metric(
            "ingester_duration",
            "ingester request query execution duration",
        );
        let ingester_duration_success = ingester_duration.recorder(&[("result", "success")]);
        let ingester_duration_error = ingester_duration.recorder(&[("result", "error")]);
        let ingester_duration_cancelled = ingester_duration.recorder(&[("result", "cancelled")]);

        Self {
            ingester_duration_success,
            ingester_duration_error,
            ingester_duration_cancelled,
        }
    }
}

/// Helper to observe a single ingester request.
///
/// Use [`set_ok`](Self::set_ok) or [`set_err`](Self::set_err) if an ingester result was observered. Otherwise the
/// request will count as "cancelled".
struct ObserveIngesterRequest<'a> {
    res: Option<Result<(), ()>>,
    t_start: Time,
    time_provider: Arc<dyn TimeProvider>,
    metrics: Arc<IngesterConnectionMetrics>,
    request: GetPartitionForIngester<'a>,
}

impl<'a> ObserveIngesterRequest<'a> {
    fn new(request: GetPartitionForIngester<'a>, metrics: Arc<IngesterConnectionMetrics>) -> Self {
        let time_provider = request.catalog_cache.time_provider();
        let t_start = time_provider.now();

        Self {
            res: None,
            t_start,
            time_provider,
            metrics,
            request,
        }
    }

    fn set_ok(mut self) {
        self.res = Some(Ok(()));
    }

    fn set_err(mut self) {
        self.res = Some(Err(()));
    }
}

impl<'a> Drop for ObserveIngesterRequest<'a> {
    fn drop(&mut self) {
        let t_end = self.time_provider.now();

        if let Some(ingester_duration) = t_end.checked_duration_since(self.t_start) {
            let (metric, status) = match self.res {
                None => (&self.metrics.ingester_duration_cancelled, "cancelled"),
                Some(Ok(())) => (&self.metrics.ingester_duration_success, "success"),
                Some(Err(())) => (&self.metrics.ingester_duration_error, "error"),
            };

            metric.record(ingester_duration);

            info!(
                predicate=?self.request.predicate,
                namespace=%self.request.namespace_name,
                table_name=%self.request.table_name,
                ?ingester_duration,
                status,
                "Time spent in ingester"
            );
        }
    }
}

/// IngesterConnection that communicates with an ingester.
#[derive(Debug)]
pub struct IngesterConnectionImpl {
    ingester_addresses: Vec<Arc<str>>,
    flight_client: Arc<dyn FlightClient>,
    catalog_cache: Arc<CatalogCache>,
    metrics: Arc<IngesterConnectionMetrics>,
}

impl IngesterConnectionImpl {
    /// Create a new connection given a Vec of `ingester_address` such as
    /// "http://127.0.0.1:8083"
    pub fn new(ingester_addresses: Vec<String>, catalog_cache: Arc<CatalogCache>) -> Self {
        Self::new_with_flight_client(
            ingester_addresses,
            Arc::new(FlightClientImpl::new()),
            catalog_cache,
        )
    }

    /// Create new ingester connection with specific flight client implementation.
    ///
    /// This is helpful for testing, i.e. when the flight client should not be backed by normal network communication.
    pub fn new_with_flight_client(
        ingester_addresses: Vec<String>,
        flight_client: Arc<dyn FlightClient>,
        catalog_cache: Arc<CatalogCache>,
    ) -> Self {
        let ingester_addresses = ingester_addresses
            .into_iter()
            .map(|addr| Arc::from(addr.as_str()))
            .collect();

        let metric_registry = catalog_cache.metric_registry();
        let metrics = Arc::new(IngesterConnectionMetrics::new(&metric_registry));

        Self {
            ingester_addresses,
            flight_client,
            catalog_cache,
            metrics,
        }
    }
}

/// Struct that names all parameters to `execute`
#[derive(Debug, Clone)]
struct GetPartitionForIngester<'a> {
    flight_client: Arc<dyn FlightClient>,
    catalog_cache: Arc<CatalogCache>,
    ingester_address: Arc<str>,
    namespace_name: Arc<str>,
    table_name: Arc<str>,
    columns: Vec<String>,
    predicate: &'a Predicate,
    expected_schema: Arc<Schema>,
}

/// Fetches the partitions for a single ingester
async fn execute(request: GetPartitionForIngester<'_>) -> Result<Vec<IngesterPartition>> {
    let GetPartitionForIngester {
        flight_client,
        catalog_cache,
        ingester_address,
        namespace_name,
        table_name,
        columns,
        predicate,
        expected_schema,
    } = request;

    let ingester_query_request = IngesterQueryRequest {
        namespace: namespace_name.to_string(),
        table: table_name.to_string(),
        columns: columns.clone(),
        predicate: Some(predicate.clone()),
    };

    let query_res = flight_client
        .query(Arc::clone(&ingester_address), ingester_query_request)
        .await;

    if let Err(FlightClientError::Flight {
        source: FlightError::GrpcError(status),
    }) = &query_res
    {
        if status.code() == tonic::Code::NotFound {
            debug!(
                ingester_address=ingester_address.as_ref(),
                %namespace_name,
                %table_name,
                "Ingester does not know namespace or table, skipping",
            );
            return Ok(vec![]);
        }
    }
    let mut perform_query = query_res
        .context(RemoteQuerySnafu {
            ingester_address: ingester_address.as_ref(),
        })
        .map_err(|e| {
            // generate a warning that is sufficient to replicate the request using CLI tooling
            warn!(
                e=%e,
                ingester_address=ingester_address.as_ref(),
                namespace=namespace_name.as_ref(),
                table=table_name.as_ref(),
                columns=columns.join(",").as_str(),
                predicate_str=%predicate,
                predicate_binary=encode_predicate_as_base64(predicate).as_str(),
                "Failed to perform ingester query",
            );

            //  need to return error until https://github.com/rust-lang/rust/issues/91345 is stable
            e
        })?;

    // read unpersisted partitions
    // map partition_id -> (PartitionMetadata, Vec<PartitionData>))
    let mut partitions: HashMap<_, _> = HashMap::new();

    // sort batches into partitions
    let mut num_batches = 0usize;
    let mut current_partition_id = None;
    while let Some((msg, md)) = perform_query
        .next()
        .await
        .map_err(|source| FlightClientError::Flight { source })
        .context(RemoteQuerySnafu {
            ingester_address: ingester_address.as_ref(),
        })?
    {
        match msg {
            LowLevelMessage::None => {
                // new partition announced
                let partition_id = md.partition_id;
                let status = md.status.context(PartitionStatusMissingSnafu {
                    partition_id,
                    ingester_address: ingester_address.as_ref(),
                })?;
                let existing = partitions.insert(partition_id, (status, vec![]));
                ensure!(
                    existing.is_none(),
                    DuplicatePartitionInfoSnafu {
                        partition_id,
                        ingester_address: ingester_address.as_ref()
                    },
                );
                current_partition_id = Some(partition_id);
            }
            LowLevelMessage::Schema(_) => {
                // can be ignored, schemas are propagated to the batches automatically by the low-level client
            }
            LowLevelMessage::RecordBatch(batch) => {
                let partition_id = current_partition_id.context(BatchWithoutPartitionSnafu {
                    ingester_address: ingester_address.as_ref(),
                })?;
                partitions
                    .get_mut(&partition_id)
                    .expect("current partition should have been inserted")
                    .1
                    .push(batch);
                num_batches += 1;
            }
        }
    }
    debug!(%ingester_address, num_batches, "Received batches from ingester");

    let mut ingester_partitions = vec![];
    for (partition_id, (state, batches)) in partitions {
        // do NOT filter out empty partitions, because the caller of this functions needs the attached metadata
        // to select the right parquet files and tombstones
        let partition_id = PartitionId::new(partition_id);
        let sequencer_id = catalog_cache.partition().sequencer_id(partition_id).await;
        let partition_sort_key = catalog_cache.partition().sort_key(partition_id).await;
        let ingester_partition = IngesterPartition::try_new(
            Arc::clone(&ingester_address),
            ChunkId::new(),
            Arc::clone(&namespace_name),
            Arc::clone(&table_name),
            partition_id,
            sequencer_id,
            // TODO(marco): project schema to the columns that are present within this partition
            Arc::clone(&expected_schema),
            state.parquet_max_sequence_number.map(SequenceNumber::new),
            state.tombstone_max_sequence_number.map(SequenceNumber::new),
            partition_sort_key,
            batches,
        )?;
        ingester_partitions.push(ingester_partition);
    }

    Ok(ingester_partitions)
}

fn encode_predicate_as_base64(predicate: &Predicate) -> String {
    use generated_types::influxdata::iox::ingester::v1::Predicate as ProtoPredicate;

    let predicate = match ProtoPredicate::try_from(predicate.clone()) {
        Ok(predicate) => predicate,
        Err(_) => {
            return String::from("<invalid>");
        }
    };

    match encode_proto_predicate_as_base64(&predicate) {
        Ok(s) => s,
        Err(_) => String::from("<encoding-error>"),
    }
}

#[async_trait]
impl IngesterConnection for IngesterConnectionImpl {
    /// Retrieve chunks from the ingester for the particular table and
    /// predicate
    async fn partitions(
        &self,
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &Predicate,
        expected_schema: Arc<Schema>,
    ) -> Result<Vec<IngesterPartition>> {
        let metrics = Arc::clone(&self.metrics);

        let mut ingester_partitions: Vec<IngesterPartition> = self
            .ingester_addresses
            .iter()
            .map(move |ingester_address| {
                let request = GetPartitionForIngester {
                    flight_client: Arc::clone(&self.flight_client),
                    catalog_cache: Arc::clone(&self.catalog_cache),
                    ingester_address: Arc::clone(ingester_address),
                    namespace_name: Arc::clone(&namespace_name),
                    table_name: Arc::clone(&table_name),
                    columns: columns.clone(),
                    predicate,
                    expected_schema: Arc::clone(&expected_schema),
                };
                let metrics = Arc::clone(&metrics);

                // wrap `execute` into an additional future so that we can measure the request time
                // INFO: create the measurement structure outside of the async block so cancellation is always measured
                let measure_me = ObserveIngesterRequest::new(request.clone(), metrics);
                async move {
                    let res = execute(request.clone()).await;

                    match &res {
                        Ok(_) => measure_me.set_ok(),
                        Err(_) => measure_me.set_err(),
                    }

                    res
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?
            // We have a Vec<Vec<..>> flatten to Vec<_>
            .into_iter()
            .flatten()
            .collect();

        ingester_partitions.sort_by_key(|p| p.partition_id);
        Ok(ingester_partitions)
    }

    async fn get_write_info(&self, write_token: &str) -> Result<GetWriteInfoResponse> {
        let responses = self
            .ingester_addresses
            .iter()
            .map(|ingester_address| execute_get_write_infos(ingester_address, write_token))
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;

        Ok(merge_responses(responses))
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

async fn execute_get_write_infos(
    ingester_address: &str,
    write_token: &str,
) -> Result<GetWriteInfoResponse, Error> {
    let connection = connection::Builder::new()
        .build(ingester_address)
        .await
        .context(ConnectingSnafu { ingester_address })?;

    influxdb_iox_client::write_info::Client::new(connection)
        .get_write_info(write_token)
        .await
        .context(WriteInfoSnafu {
            ingester_address,
            write_token,
        })
}

/// A wrapper around the unpersisted data in a partition returned by
/// the ingester that (will) implement the `QueryChunk` interface
///
/// Given the catalog heirarchy:
///
/// ```text
/// (Catalog) Sequencer -> (Catalog) Table --> (Catalog) Partition
/// ```
///
/// An IngesterPartition contains the unpersisted data for a catalog
/// partition from a sequencer. Thus, there can be more than one
/// IngesterPartition for each table the ingester knows about.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct IngesterPartition {
    ingester: Arc<str>,
    chunk_id: ChunkId,
    #[allow(dead_code)]
    namespace_name: Arc<str>,
    table_name: Arc<str>,
    partition_id: PartitionId,
    sequencer_id: SequencerId,

    schema: Arc<Schema>,

    /// Maximum sequence number of parquet files the ingester has
    /// persisted for this partition
    parquet_max_sequence_number: Option<SequenceNumber>,

    /// Maximum sequence number of tombstone that the ingester has
    /// persisted for this partition
    tombstone_max_sequence_number: Option<SequenceNumber>,

    /// Partition-wide sort key.
    partition_sort_key: Arc<Option<SortKey>>,

    /// The raw table data
    batches: Vec<RecordBatch>,

    /// Summary Statistics
    summary: TableSummary,
}

impl IngesterPartition {
    /// Creates a new IngesterPartition, translating the passed
    /// `RecordBatches` into the correct types
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        ingester: Arc<str>,
        chunk_id: ChunkId,
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        partition_id: PartitionId,
        sequencer_id: SequencerId,
        expected_schema: Arc<Schema>,
        parquet_max_sequence_number: Option<SequenceNumber>,
        tombstone_max_sequence_number: Option<SequenceNumber>,
        partition_sort_key: Arc<Option<SortKey>>,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        // ensure that the schema of the batches matches the required
        // output schema by CAST'ing to the needed type.
        //
        // This is needed because the flight client doesn't send
        // dictionaries (see comments on ensure_schema for more
        // details)
        let batches = batches
            .into_iter()
            .map(|batch| ensure_schema(batch, expected_schema.as_ref()))
            .collect::<Result<Vec<RecordBatch>>>()?;

        let summary = calculate_summary(&batches, &expected_schema);

        Ok(Self {
            ingester,
            chunk_id,
            namespace_name,
            table_name,
            partition_id,
            sequencer_id,
            schema: expected_schema,
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            partition_sort_key,
            batches,
            summary,
        })
    }

    pub(crate) fn with_partition_sort_key(self, partition_sort_key: Arc<Option<SortKey>>) -> Self {
        Self {
            partition_sort_key,
            ..self
        }
    }

    pub(crate) fn ingester(&self) -> &Arc<str> {
        &self.ingester
    }

    pub(crate) fn has_batches(&self) -> bool {
        !self.batches.is_empty()
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub(crate) fn sequencer_id(&self) -> SequencerId {
        self.sequencer_id
    }

    pub(crate) fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.parquet_max_sequence_number
    }

    pub(crate) fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.tombstone_max_sequence_number
    }
}

impl QueryChunkMeta for IngesterPartition {
    fn summary(&self) -> Option<&TableSummary> {
        Some(&self.summary)
    }

    fn schema(&self) -> Arc<Schema> {
        trace!(schema=?self.schema, "IngesterPartition schema");
        Arc::clone(&self.schema)
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().as_ref()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        Some(self.partition_id())
    }

    fn sort_key(&self) -> Option<&SortKey> {
        //Some(&self.sort_key)
        // Data is not sorted
        None
    }

    fn delete_predicates(&self) -> &[Arc<data_types::DeletePredicate>] {
        &[]
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        // TODO: may want to ask the Ingester to send this value instead of computing it here.
        // Note: if we return None here, this chunk will be considered ovelapped with all other chunks
        // even if it does not and lead to unecessary deduplication
        let (min, max) =
            compute_timenanosecond_min_max(&self.batches).expect("Should have time range");

        Some(TimestampMinMax { min, max })
    }
}

impl QueryChunk for IngesterPartition {
    fn id(&self) -> ChunkId {
        self.chunk_id
    }

    fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        // ingester runs dedup before creating the record batches so
        // when the querier gets them they have no duplicates
        false
    }

    fn apply_predicate_to_metadata(
        &self,
        _predicate: &Predicate,
    ) -> Result<PredicateMatch, QueryChunkError> {
        // TODO maybe some special handling?
        Ok(PredicateMatch::Unknown)
    }

    fn column_names(
        &self,
        _ctx: IOxSessionContext,
        _predicate: &Predicate,
        _columns: Selection<'_>,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        // TODO maybe some special handling?
        Ok(None)
    }

    fn column_values(
        &self,
        _ctx: IOxSessionContext,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        // TODO maybe some special handling?
        Ok(None)
    }

    fn read_filter(
        &self,
        _ctx: IOxSessionContext,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, QueryChunkError> {
        trace!(?predicate, ?selection, input_batches=?self.batches, "Reading data");

        let schema = self.schema();
        let stream = MemoryStream::try_new(
            self.batches.clone(),
            schema.as_arrow(),
            schema.df_projection(selection)?,
        )
        .expect("Creating memory stream");

        Ok(Box::pin(stream))
    }

    fn chunk_type(&self) -> &str {
        "IngesterPartition"
    }

    fn order(&self) -> ChunkOrder {
        // since this is always the 'most recent' chunk for this
        // partition, put it at the end
        ChunkOrder::new(u32::MAX).unwrap()
    }
}

/// Ensure that the record batch has the given schema.
///
/// This does multiple things:
///
/// 1. Dictionary type recovery
/// 2. NULL-column creation
///
/// # Dictionary Type Recovery
///
/// Cast arrays in record batch to be the type of schema. This is a workaround for
/// <https://github.com/influxdata/influxdb_iox/pull/4273> where the Flight API doesn't necessarily
/// return the same schema as was provided by the ingester.
///
/// Namely, dictionary encoded columns (e.g. tags) are returned as `DataType::Utf8` even when they
/// were sent as `DataType::Dictionary(Int32, Utf8)`.
///
/// # NULL-column Creation
///
/// If a column is absent in an ingester partition it will not be part of record batch even when
/// the querier requests it. In that case we create it as "all NULL" column with the appropriate
/// type.
///
/// An alternative would be to remove the column from the schema of the appropriate
/// [`IngesterPartition`]. However, since a partition may contain multiple record batches and we do
/// not want to assume that the presence/absence of columns is identical for all of them, we fix
/// this here.
fn ensure_schema(batch: RecordBatch, expected_schema: &Schema) -> Result<RecordBatch> {
    let actual_schema = batch.schema();
    let desired_fields = expected_schema.iter().map(|(_, f)| f);

    let new_columns = desired_fields
        .map(|desired_field| {
            let desired_type = desired_field.data_type();

            // find column by name
            match actual_schema.column_with_name(desired_field.name()) {
                Some((idx, _field)) => {
                    let col = batch.column(idx);
                    let actual_type = col.data_type();

                    // check type
                    if desired_type != actual_type {
                        if let DataType::Dictionary(_key_type, value_type) = desired_type.clone() {
                            if value_type.as_ref() == actual_type {
                                // convert
                                return arrow::compute::cast(col, desired_type).context(
                                    ConvertingRecordBatchSnafu {
                                        column_name: desired_field.name(),
                                        data_type: desired_type.clone(),
                                    },
                                );
                            }
                        }

                        RecordBatchTypeSnafu {
                            column_name: desired_field.name(),
                            actual_data_type: actual_type.clone(),
                            desired_data_type: desired_type.clone(),
                        }
                        .fail()
                    } else {
                        Ok(Arc::clone(col))
                    }
                }
                None => Ok(arrow::array::new_null_array(desired_type, batch.num_rows())),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(expected_schema.as_arrow(), new_columns).context(CreatingRecordBatchSnafu)
}

fn calculate_summary(batches: &[RecordBatch], schema: &Schema) -> TableSummary {
    let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64;

    let mut columns = Vec::with_capacity(schema.len());
    for i in 0..schema.len() {
        let (t, field) = schema.field(i);
        let t = t.expect("influx column type must be known");

        let influxdb_type = match t {
            InfluxColumnType::Tag => InfluxDbType::Tag,
            InfluxColumnType::Field(_) => InfluxDbType::Field,
            InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
        };

        let stats = match t {
            InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String) => {
                Statistics::String(StatValues {
                    min: None,
                    max: None,
                    total_count: row_count,
                    null_count: None,
                    distinct_count: None,
                })
            }
            InfluxColumnType::Timestamp | InfluxColumnType::Field(InfluxFieldType::Integer) => {
                Statistics::I64(StatValues {
                    min: None,
                    max: None,
                    total_count: row_count,
                    null_count: None,
                    distinct_count: None,
                })
            }
            InfluxColumnType::Field(InfluxFieldType::UInteger) => Statistics::U64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Float) => Statistics::F64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Boolean) => Statistics::Bool(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
        };

        columns.push(ColumnSummary {
            name: field.name().clone(),
            influxdb_type: Some(influxdb_type),
            stats,
        })
    }

    TableSummary { columns }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::{
        array::{ArrayRef, DictionaryArray, Int64Array, StringArray, TimestampNanosecondArray},
        datatypes::Int32Type,
    };
    use assert_matches::assert_matches;
    use generated_types::influxdata::iox::ingester::v1::PartitionStatus;
    use influxdb_iox_client::flight::generated_types::IngesterQueryResponseMetadata;
    use iox_tests::util::TestCatalog;
    use metric::Attributes;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use schema::{builder::SchemaBuilder, InfluxFieldType};
    use tokio::sync::Mutex;

    use super::{flight_client::QueryData, *};

    #[tokio::test]
    async fn test_flight_handshake_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Err(FlightClientError::Handshake {
                    ingester_address: String::from("addr1"),
                    source: FlightError::GrpcError(tonic::Status::internal("don't know")),
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_internal_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Err(FlightClientError::Flight {
                    source: FlightError::GrpcError(tonic::Status::internal("cow exploded")),
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_not_found() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Err(FlightClientError::Flight {
                    source: FlightError::GrpcError(tonic::Status::not_found("something")),
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let partitions = get_partitions(&ingester_conn).await.unwrap();
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_flight_stream_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Err(FlightError::GrpcError(tonic::Status::internal(
                        "don't know",
                    )))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_no_partitions() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([("addr1", Ok(MockQueryData { results: vec![] }))]).await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let partitions = get_partitions(&ingester_conn).await.unwrap();
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_flight_no_batches() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Ok((
                        LowLevelMessage::None,
                        IngesterQueryResponseMetadata {
                            partition_id: 1,
                            status: Some(PartitionStatus {
                                parquet_max_sequence_number: None,
                                tombstone_max_sequence_number: None,
                            }),
                        },
                    ))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        let partitions = get_partitions(&ingester_conn).await.unwrap();
        assert_eq!(partitions.len(), 1);

        let p = &partitions[0];
        assert_eq!(p.partition_id.get(), 1);
        assert_eq!(p.sequencer_id.get(), 1);
        assert_eq!(p.parquet_max_sequence_number, None);
        assert_eq!(p.tombstone_max_sequence_number, None);
        assert_eq!(p.batches.len(), 0);
    }

    #[tokio::test]
    async fn test_flight_err_partition_status_missing() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Ok((
                        LowLevelMessage::None,
                        IngesterQueryResponseMetadata {
                            partition_id: 1,
                            status: None,
                        },
                    ))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::PartitionStatusMissing { .. });
    }

    #[tokio::test]
    async fn test_flight_err_duplicate_partition_info() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![
                        Ok((
                            LowLevelMessage::None,
                            IngesterQueryResponseMetadata {
                                partition_id: 1,
                                status: Some(PartitionStatus {
                                    parquet_max_sequence_number: None,
                                    tombstone_max_sequence_number: None,
                                }),
                            },
                        )),
                        Ok((
                            LowLevelMessage::None,
                            IngesterQueryResponseMetadata {
                                partition_id: 2,
                                status: Some(PartitionStatus {
                                    parquet_max_sequence_number: None,
                                    tombstone_max_sequence_number: None,
                                }),
                            },
                        )),
                        Ok((
                            LowLevelMessage::None,
                            IngesterQueryResponseMetadata {
                                partition_id: 1,
                                status: Some(PartitionStatus {
                                    parquet_max_sequence_number: None,
                                    tombstone_max_sequence_number: None,
                                }),
                            },
                        )),
                    ],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::DuplicatePartitionInfo { .. });
    }

    #[tokio::test]
    async fn test_flight_err_batch_without_partition() {
        let record_batch = lp_to_record_batch("table foo=1 1");
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Ok((
                        LowLevelMessage::RecordBatch(record_batch),
                        IngesterQueryResponseMetadata::default(),
                    ))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::BatchWithoutPartition { .. });
    }

    #[tokio::test]
    async fn test_flight_many_batches() {
        let record_batch_1_1 = lp_to_record_batch("table foo=1 1");
        let record_batch_1_2 = lp_to_record_batch("table bar=20,foo=2 2");
        let record_batch_2_1 = lp_to_record_batch("table foo=3 3");
        let record_batch_3_1 = lp_to_record_batch("table baz=40,foo=4 4");

        let schema_1_1 = record_batch_1_1.schema();
        let schema_1_2 = record_batch_1_2.schema();
        let schema_2_1 = record_batch_2_1.schema();
        let schema_3_1 = record_batch_3_1.schema();

        let mock_flight_client = Arc::new(
            MockFlightClient::new([
                (
                    "addr1",
                    Ok(MockQueryData {
                        results: vec![
                            Ok((
                                LowLevelMessage::None,
                                IngesterQueryResponseMetadata {
                                    partition_id: 1,
                                    status: Some(PartitionStatus {
                                        parquet_max_sequence_number: Some(11),
                                        tombstone_max_sequence_number: Some(12),
                                    }),
                                },
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_1_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_1_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_1_2)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_1_2),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::None,
                                IngesterQueryResponseMetadata {
                                    partition_id: 2,
                                    status: Some(PartitionStatus {
                                        parquet_max_sequence_number: Some(21),
                                        tombstone_max_sequence_number: Some(22),
                                    }),
                                },
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_2_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_2_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                        ],
                    }),
                ),
                (
                    "addr2",
                    Ok(MockQueryData {
                        results: vec![
                            Ok((
                                LowLevelMessage::None,
                                IngesterQueryResponseMetadata {
                                    partition_id: 3,
                                    status: Some(PartitionStatus {
                                        parquet_max_sequence_number: Some(31),
                                        tombstone_max_sequence_number: Some(32),
                                    }),
                                },
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_3_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_3_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                        ],
                    }),
                ),
            ])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        let partitions = get_partitions(&ingester_conn).await.unwrap();
        assert_eq!(partitions.len(), 3);

        let p1 = &partitions[0];
        assert_eq!(p1.partition_id.get(), 1);
        assert_eq!(p1.sequencer_id.get(), 1);
        assert_eq!(
            p1.parquet_max_sequence_number,
            Some(SequenceNumber::new(11))
        );
        assert_eq!(
            p1.tombstone_max_sequence_number,
            Some(SequenceNumber::new(12))
        );
        assert_eq!(p1.batches.len(), 2);
        assert_eq!(p1.batches[0].schema(), schema().as_arrow());
        assert_eq!(p1.batches[1].schema(), schema().as_arrow());

        let p2 = &partitions[1];
        assert_eq!(p2.partition_id.get(), 2);
        assert_eq!(p2.sequencer_id.get(), 1);
        assert_eq!(
            p2.parquet_max_sequence_number,
            Some(SequenceNumber::new(21))
        );
        assert_eq!(
            p2.tombstone_max_sequence_number,
            Some(SequenceNumber::new(22))
        );
        assert_eq!(p2.batches.len(), 1);
        assert_eq!(p2.batches[0].schema(), schema().as_arrow());

        let p3 = &partitions[2];
        assert_eq!(p3.partition_id.get(), 3);
        assert_eq!(p3.sequencer_id.get(), 2);
        assert_eq!(
            p3.parquet_max_sequence_number,
            Some(SequenceNumber::new(31))
        );
        assert_eq!(
            p3.tombstone_max_sequence_number,
            Some(SequenceNumber::new(32))
        );
        assert_eq!(p3.batches.len(), 1);
        assert_eq!(p3.batches[0].schema(), schema().as_arrow());
    }

    #[tokio::test]
    async fn test_ingester_metrics() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([
                ("addr1", Ok(MockQueryData { results: vec![] })),
                ("addr2", Ok(MockQueryData { results: vec![] })),
                (
                    "addr3",
                    Err(FlightClientError::Handshake {
                        ingester_address: String::from("addr3"),
                        source: FlightError::GrpcError(tonic::Status::internal("don't know")),
                    }),
                ),
                (
                    "addr4",
                    Err(FlightClientError::Handshake {
                        ingester_address: String::from("addr4"),
                        source: FlightError::GrpcError(tonic::Status::internal("don't know")),
                    }),
                ),
                ("addr5", Ok(MockQueryData { results: vec![] })),
            ])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        get_partitions(&ingester_conn).await.ok();

        let histogram_error = mock_flight_client
            .catalog
            .metric_registry()
            .get_instrument::<Metric<DurationHistogram>>("ingester_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("result", "error")]))
            .expect("failed to get observer")
            .fetch();

        // only one error got counted because the other got cancelled
        let hit_count_error = histogram_error.sample_count();
        assert_eq!(hit_count_error, 1);

        let histogram_success = mock_flight_client
            .catalog
            .metric_registry()
            .get_instrument::<Metric<DurationHistogram>>("ingester_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count_success = histogram_success.sample_count();

        let histogram_cancelled = mock_flight_client
            .catalog
            .metric_registry()
            .get_instrument::<Metric<DurationHistogram>>("ingester_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("result", "cancelled")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count_cancelled = histogram_cancelled.sample_count();

        // we don't know how errors are propagated because the futures are polled unordered
        assert_eq!(hit_count_success + hit_count_cancelled, 4);
    }

    async fn get_partitions(
        ingester_conn: &IngesterConnectionImpl,
    ) -> Result<Vec<IngesterPartition>, Error> {
        let namespace = Arc::from("namespace");
        let table = Arc::from("table");
        let columns = vec![String::from("col")];
        let schema = schema();
        ingester_conn
            .partitions(namespace, table, columns, &Predicate::default(), schema)
            .await
    }

    fn schema() -> Arc<Schema> {
        Arc::new(
            SchemaBuilder::new()
                .influx_field("bar", InfluxFieldType::Float)
                .influx_field("baz", InfluxFieldType::Float)
                .influx_field("foo", InfluxFieldType::Float)
                .timestamp()
                .build()
                .unwrap(),
        )
    }

    fn lp_to_record_batch(lp: &str) -> RecordBatch {
        lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
    }

    #[derive(Debug)]
    struct MockQueryData {
        results: Vec<Result<(LowLevelMessage, IngesterQueryResponseMetadata), FlightError>>,
    }

    #[async_trait]
    impl QueryData for MockQueryData {
        async fn next(
            &mut self,
        ) -> Result<Option<(LowLevelMessage, IngesterQueryResponseMetadata)>, FlightError> {
            if self.results.is_empty() {
                Ok(None)
            } else {
                self.results.remove(0).map(Some)
            }
        }
    }

    #[derive(Debug)]
    struct MockFlightClient {
        catalog: Arc<TestCatalog>,
        responses: Mutex<HashMap<String, Result<MockQueryData, FlightClientError>>>,
    }

    impl MockFlightClient {
        async fn new<const N: usize>(
            responses: [(&'static str, Result<MockQueryData, FlightClientError>); N],
        ) -> Self {
            let catalog = TestCatalog::new();
            let ns = catalog.create_namespace("namespace").await;
            let table = ns.create_table("table").await;

            let s0 = ns.create_sequencer(0).await;
            let s1 = ns.create_sequencer(1).await;

            table.with_sequencer(&s0).create_partition("k1").await;
            table.with_sequencer(&s0).create_partition("k2").await;
            table.with_sequencer(&s1).create_partition("k3").await;

            Self {
                catalog,
                responses: Mutex::new(
                    responses
                        .into_iter()
                        .map(|(k, v)| (String::from(k), v))
                        .collect(),
                ),
            }
        }

        async fn ingester_conn(self: &Arc<Self>) -> IngesterConnectionImpl {
            let ingester_addresses = self.responses.lock().await.keys().cloned().collect();
            IngesterConnectionImpl::new_with_flight_client(
                ingester_addresses,
                Arc::clone(self) as _,
                Arc::new(CatalogCache::new(
                    self.catalog.catalog(),
                    self.catalog.time_provider(),
                    self.catalog.metric_registry(),
                    usize::MAX,
                )),
            )
        }
    }

    #[async_trait]
    impl FlightClient for MockFlightClient {
        async fn query(
            &self,
            ingester_address: Arc<str>,
            _request: IngesterQueryRequest,
        ) -> Result<Box<dyn QueryData>, FlightClientError> {
            self.responses
                .lock()
                .await
                .remove(ingester_address.as_ref())
                .expect("Response not mocked")
                .map(|query_data| Box::new(query_data) as _)
        }
    }

    #[test]
    fn test_ingester_partition_type_cast() {
        let expected_schema = Arc::new(SchemaBuilder::new().tag("t").timestamp().build().unwrap());

        let cases = vec![
            // send a batch that matches the schema exactly
            RecordBatch::try_from_iter(vec![("t", dict_array()), ("time", ts_array())]).unwrap(),
            // Model what the ingester sends (dictionary decoded to string)
            RecordBatch::try_from_iter(vec![("t", string_array()), ("time", ts_array())]).unwrap(),
        ];

        for case in cases {
            let parquet_max_sequence_number = None;
            let tombstone_max_sequence_number = None;
            // Construct a partition and ensure it doesn't error
            let ingester_partition = IngesterPartition::try_new(
                "ingester".into(),
                ChunkId::new(),
                "ns".into(),
                "table".into(),
                PartitionId::new(1),
                SequencerId::new(1),
                Arc::clone(&expected_schema),
                parquet_max_sequence_number,
                tombstone_max_sequence_number,
                Arc::new(None),
                vec![case],
            )
            .unwrap();

            for batch in &ingester_partition.batches {
                assert_eq!(batch.schema(), expected_schema.as_arrow());
            }
        }
    }

    #[test]
    fn test_ingester_partition_fail_type_cast() {
        let expected_schema = Arc::new(
            SchemaBuilder::new()
                .field("b", DataType::Boolean)
                .timestamp()
                .build()
                .unwrap(),
        );

        let batch =
            RecordBatch::try_from_iter(vec![("b", int64_array()), ("time", ts_array())]).unwrap();

        let parquet_max_sequence_number = None;
        let tombstone_max_sequence_number = None;
        let err = IngesterPartition::try_new(
            "ingester".into(),
            ChunkId::new(),
            "ns".into(),
            "table".into(),
            PartitionId::new(1),
            SequencerId::new(1),
            Arc::clone(&expected_schema),
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            Arc::new(None),
            vec![batch],
        )
        .unwrap_err();

        assert_matches!(err, Error::RecordBatchType { .. });
    }

    #[test]
    fn test_ingester_partition_null_column() {
        let expected_schema = Arc::new(
            SchemaBuilder::new()
                .field("b", DataType::Boolean)
                .timestamp()
                .build()
                .unwrap(),
        );

        let batch = RecordBatch::try_from_iter(vec![("time", ts_array())]).unwrap();

        let parquet_max_sequence_number = None;
        let tombstone_max_sequence_number = None;
        let ingester_partition = IngesterPartition::try_new(
            "ingester".into(),
            ChunkId::new(),
            "ns".into(),
            "table".into(),
            PartitionId::new(1),
            SequencerId::new(1),
            Arc::clone(&expected_schema),
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            Arc::new(None),
            vec![batch],
        )
        .unwrap();

        assert_eq!(ingester_partition.batches.len(), 1);
        let batch = &ingester_partition.batches[0];
        assert_eq!(batch.column(0).data_type(), &DataType::Boolean);
        assert!(arrow::compute::is_null(batch.column(0))
            .unwrap()
            .into_iter()
            .all(|x| x.unwrap()));
    }

    fn ts_array() -> ArrayRef {
        Arc::new(
            [Some(1), Some(2), Some(3)]
                .iter()
                .collect::<TimestampNanosecondArray>(),
        )
    }

    fn string_array() -> ArrayRef {
        Arc::new(str_vec().iter().collect::<StringArray>())
    }

    fn dict_array() -> ArrayRef {
        Arc::new(
            str_vec()
                .iter()
                .copied()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn int64_array() -> ArrayRef {
        Arc::new(i64_vec().iter().collect::<Int64Array>())
    }

    fn str_vec() -> &'static [Option<&'static str>] {
        &[Some("foo"), Some("bar"), Some("baz")]
    }

    fn i64_vec() -> &'static [Option<i64>] {
        &[Some(1), Some(2), Some(3)]
    }

    #[test]
    fn test_calculate_summary_no_columns_no_rows() {
        let schema = SchemaBuilder::new().build().unwrap();

        let actual = calculate_summary(&[], &schema);
        let expected = TableSummary { columns: vec![] };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_calculate_summary_no_rows() {
        let schema = full_schema();

        let actual = calculate_summary(&[], &schema);
        let expected = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: String::from("tag"),
                    influxdb_type: Some(InfluxDbType::Tag),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_bool"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::Bool(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_float"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::F64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_integer"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_string"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_uinteger"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::U64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("time"),
                    influxdb_type: Some(InfluxDbType::Timestamp),
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_calculate_summary() {
        let schema = full_schema();
        let batches = &[
            lp_to_record_batch("table,tag=foo field_bool=true,field_float=1.1,field_integer=1,field_string=\"bar\",field_uinteger=2u 42"),
            lp_to_record_batch(&[
                "table,tag=foo field_bool=true,field_float=1.1,field_integer=1,field_string=\"bar\",field_uinteger=2u 42",
                "table,tag=foo field_bool=true,field_float=1.1,field_integer=1,field_string=\"bar\",field_uinteger=2u 42",
            ].join("\n")),
        ];

        let actual = calculate_summary(batches, &schema);
        let expected = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: String::from("tag"),
                    influxdb_type: Some(InfluxDbType::Tag),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_bool"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::Bool(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_float"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::F64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_integer"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_string"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_uinteger"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::U64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("time"),
                    influxdb_type: Some(InfluxDbType::Timestamp),
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    fn full_schema() -> Schema {
        SchemaBuilder::new()
            .tag("tag")
            .influx_field("field_bool", InfluxFieldType::Boolean)
            .influx_field("field_float", InfluxFieldType::Float)
            .influx_field("field_integer", InfluxFieldType::Integer)
            .influx_field("field_string", InfluxFieldType::String)
            .influx_field("field_uinteger", InfluxFieldType::UInteger)
            .timestamp()
            .build()
            .unwrap()
    }
}
