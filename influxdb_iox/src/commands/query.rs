use influxdb_iox_client::{
    connection::Connection,
    flight::{self, generated_types::ReadInfo},
    format::QueryOutputFormat,
};
use std::str::FromStr;
use thiserror::Error;
use generated_types::influxdata::iox::querier::v1::read_info::QueryString::{InfluxqlQuery, SqlQuery};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    Formatting(#[from] influxdb_iox_client::format::Error),

    #[error("Invalid dialect: `{0}`")]
    Dialect(String),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Query the data with SQL
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The IOx namespace to query
    #[clap(action)]
    namespace: String,

    /// The query to run, in SQL format
    #[clap(action)]
    query: String,

    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty", action)]
    format: String,

    /// Optional dialect ('sql', or 'influxql')
    #[clap(short, long, default_value = "sql", action)]
    dialect: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = flight::Client::new(connection);
    let Config {
        namespace,
        format,
        query,
        dialect,
    } = config;

    let format = QueryOutputFormat::from_str(&format)?;

    let query_string = match dialect.as_str() {
        "sql" => Some(SqlQuery(query)),
        "influxql" => Some(InfluxqlQuery(query)),
        _ => return Err(Error::Dialect(dialect))
    };

    let mut query_results = client
        .perform_query(ReadInfo {
            namespace_name: namespace,
            query_string,
        })
        .await?;

    // It might be nice to do some sort of streaming write
    // rather than buffering the whole thing.
    let mut batches = vec![];
    while let Some(data) = query_results.next().await? {
        batches.push(data);
    }

    let formatted_result = format.format(&batches)?;

    println!("{}", formatted_result);

    Ok(())
}
