//! Implementation of command line option for running server

use std::sync::Arc;

use crate::{
    influxdb_ioxd::{
        self,
        server_type::{
            common_state::{CommonServerState, CommonServerStateError},
            router::RouterServerType,
        },
    },
    structopt_blocks::run_config::RunConfig,
};
use router::server::RouterServer;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Cannot setup server: {0}")]
    Setup(#[from] crate::influxdb_ioxd::server_type::database::setup::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "run",
    about = "Runs in router mode",
    long_about = "Run the IOx router server.\n\nThe configuration options below can be \
    set either with the command line flags or with the specified environment \
    variable. If there is a file named '.env' in the current working directory, \
    it is sourced before loading the configuration.

Configuration is loaded from the following sources (highest precedence first):
        - command line arguments
        - user set environment variables
        - .env file contents
        - pre-configured default values"
)]
pub struct Config {
    #[structopt(flatten)]
    pub(crate) run_config: RunConfig,

    /// When IOx nodes need to talk to remote peers they consult an internal remote address
    /// mapping. This mapping is populated via API calls. If the mapping doesn't produce
    /// a result, this config entry allows to generate a hostname from at template:
    /// occurrences of the "{id}" substring will be replaced with the remote Server ID.
    ///
    /// Example: http://node-{id}.ioxmydomain.com:8082
    #[structopt(long = "--remote-template", env = "INFLUXDB_IOX_REMOTE_TEMPLATE")]
    pub remote_template: Option<String>,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let router_server = Arc::new(RouterServer::new(common_state.trace_collector()));
    let server_type = Arc::new(RouterServerType::new(router_server, &common_state));

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}