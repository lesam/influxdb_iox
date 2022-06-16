use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::{
    collections::{BTreeSet, HashMap},
    fs, io,
    path::PathBuf,
    sync::Arc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ingester address '{}' was repeated", ingester_address))]
    RepeatedAddress { ingester_address: String },

    #[snafu(display("Could not read sequencer to ingester file `{}`: {source}", file.display()))]
    SequencerToIngesterFileReading { source: io::Error, file: PathBuf },

    #[snafu(display("Could not deserialize JSON from ingester file: {source}"))]
    SequencerToIngesterDeserializing { source: serde_json::Error },
}

/// CLI config for querier configuration
#[derive(Debug, Clone, PartialEq, clap::Parser)]
pub struct QuerierConfig {
    /// The number of threads to use for queries.
    ///
    /// If not specified, defaults to the number of cores on the system
    #[clap(
        long = "--num-query-threads",
        env = "INFLUXDB_IOX_NUM_QUERY_THREADS",
        action
    )]
    pub num_query_threads: Option<usize>,

    /// gRPC address for the querier to talk with the ingester. For
    /// example:
    ///
    /// "http://127.0.0.1:8083"
    ///
    /// or
    ///
    /// "http://10.10.10.1:8083,http://10.10.10.2:8083"
    ///
    /// for multiple addresses.
    ///
    /// Note we plan to improve this interface in
    /// <https://github.com/influxdata/influxdb_iox/issues/3996>
    #[clap(
        long = "--ingester-address",
        env = "INFLUXDB_IOX_INGESTER_ADDRESSES",
        multiple_values = true,
        use_value_delimiter = true,
        action
    )]
    pub ingester_addresses: Vec<String>,

    /// Path to a JSON file containing a Sequencer ID to ingester gRPC mapping. For example:
    ///
    /// ```json
    /// {
    ///    "sequencers": {
    ///      "0": { "addr": "http://ingester-1:8082" },
    ///      "1": { "addr": "http://ingester-1:8082" },
    ///      ...
    ///      "25": { "addr": "http://ingester-2:8082" }
    ///    }
    /// }
    /// ```
    #[clap(
        long = "--sequencer-to-ingester-file",
        env = "INFLUXDB_IOX_SEQUENCER_TO_INGESTER_FILE",
        action
    )]
    pub sequencer_to_ingester_file: Option<PathBuf>,

    /// Size of the RAM cache pool in bytes.
    #[clap(
        long = "--ram-pool-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_BYTES",
        default_value = "1073741824",
        action
    )]
    pub ram_pool_bytes: usize,

    /// Limit the number of concurrent queries.
    #[clap(
        long = "--max-concurrent-queries",
        env = "INFLUXDB_IOX_MAX_CONCURRENT_QUERIES",
        default_value = "10",
        action
    )]
    pub max_concurrent_queries: usize,
}

impl QuerierConfig {
    /// Get the querier config's num query threads.
    #[must_use]
    pub fn num_query_threads(&self) -> Option<usize> {
        self.num_query_threads
    }

    /// Return the querier config's ingester addresses. If `--ingester-address` is used to specify
    /// a list of addresses, this returns `Err` if any of the addresses are repeated. If
    /// `--sequencer-to-ingester-file` is used to specify a JSON file containing sequencer to
    /// ingester address mappings, this returns `Err` if there are any problems reading or
    /// deserializing the file.
    pub fn ingester_addresses(&self) -> Result<IngesterAddresses, Error> {
        if let Some(file) = &self.sequencer_to_ingester_file {
            let contents =
                fs::read_to_string(file).context(SequencerToIngesterFileReadingSnafu { file })?;
            let map = deserialize_sequencer_ingester_map(&contents)?;
            Ok(IngesterAddresses::BySequencer(map))
        } else {
            let mut current_addresses = BTreeSet::new();
            Ok(IngesterAddresses::List(
                self.ingester_addresses
                    .iter()
                    .map(|ingester_address| {
                        if current_addresses.contains(ingester_address) {
                            RepeatedAddressSnafu { ingester_address }.fail()
                        } else {
                            current_addresses.insert(ingester_address);
                            Ok(ingester_address.clone())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
    }

    /// Size of the RAM cache pool in bytes.
    pub fn ram_pool_bytes(&self) -> usize {
        self.ram_pool_bytes
    }

    /// Number of queries allowed to run concurrently
    pub fn max_concurrent_queries(&self) -> usize {
        self.max_concurrent_queries
    }
}

fn deserialize_sequencer_ingester_map(contents: &str) -> Result<HashMap<i32, Arc<str>>, Error> {
    let ingesters_config: IngestersConfig =
        serde_json::from_str(contents).context(SequencerToIngesterDeserializingSnafu)?;

    Ok(ingesters_config
        .sequencers
        .into_iter()
        .map(|(seq_id, ingester)| (seq_id, ingester.addr))
        .collect())
}

/// Either specify a list of ingester addresses or a mapping from sequencer ID to ingester
#[derive(Debug)]
pub enum IngesterAddresses {
    List(Vec<String>),
    BySequencer(HashMap<i32, Arc<str>>),
}

#[derive(Debug, Deserialize)]
struct IngestersConfig {
    sequencers: HashMap<i32, Ingester>,
}

#[derive(Debug, Deserialize)]
struct Ingester {
    addr: Arc<str>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::StructOpt;
    use test_helpers::assert_error;

    #[test]
    fn test_default() {
        let actual = QuerierConfig::try_parse_from(["my_binary"]).unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert!(matches!(
            actual.ingester_addresses().unwrap(),
            IngesterAddresses::List(list) if list.is_empty(),
        ));
    }

    #[test]
    fn test_num_threads() {
        let actual =
            QuerierConfig::try_parse_from(["my_binary", "--num-query-threads", "42"]).unwrap();

        assert_eq!(actual.num_query_threads(), Some(42));
        assert!(matches!(
            actual.ingester_addresses().unwrap(),
            IngesterAddresses::List(list) if list.is_empty(),
        ));
    }

    #[test]
    fn test_one_ingester_address() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-address",
            "http://127.0.0.1:9090",
        ])
        .unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert!(matches!(
            actual.ingester_addresses().unwrap(),
            IngesterAddresses::List(list) if list == ["http://127.0.0.1:9090".to_string()],
        ));
    }

    #[test]
    fn test_multiple_ingester_addresses() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-address",
            "http://127.0.0.1:9090,http://10.10.2.11:8080",
        ])
        .unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert!(matches!(
            actual.ingester_addresses().unwrap(),
            IngesterAddresses::List(list) if list == [
                "http://127.0.0.1:9090".to_string(),
                "http://10.10.2.11:8080".to_string()
            ],
        ));
    }

    #[test]
    fn test_multiple_ingester_addresses_repeated() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-address",
            "http://127.0.0.1:9090,http://10.10.2.11:8080,http://127.0.0.1:9090",
        ])
        .unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert_eq!(
            actual.ingester_addresses().unwrap_err().to_string(),
            "ingester address 'http://127.0.0.1:9090' was repeated"
        );
    }

    #[test]
    fn successful_deserialization() {
        let contents = r#"{
              "sequencers": {
                 "0": { "addr": "http://ingester-1:8082" },
                 "1": { "addr": "http://ingester-1:8082" },
                 "25": { "addr": "http://ingester-2:8082" }
              }
            }"#;
        let map = deserialize_sequencer_ingester_map(contents).unwrap();

        let expected = [
            (0, "http://ingester-1:8082"),
            (1, "http://ingester-1:8082"),
            (25, "http://ingester-2:8082"),
        ]
        .into_iter()
        .map(|(seq_id, addr)| (seq_id, Arc::from(addr)))
        .collect();

        assert_eq!(map, expected);
    }

    #[test]
    fn unsuccessful_deserialization() {
        let map = deserialize_sequencer_ingester_map("");
        assert_error!(map, Error::SequencerToIngesterDeserializing { .. });
    }
}
