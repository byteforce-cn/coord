#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use clap::Parser;
use cli::{Cli, Command, init_tracing};

mod application;
mod cli;
mod ctl;
mod http_api;
mod interceptors;
mod modes;
mod persistence;
mod raft_internal;
mod raft_runtime;
mod raft_store;
mod services;
mod telemetry;
mod wire;
mod workflow_adapters;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Server(args) => {
            init_tracing(false);
            modes::server::run(args, false).await
        }
        Command::Dev(args) => {
            init_tracing(true);
            modes::server::run(args, true).await
        }
        Command::Client(args) => {
            init_tracing(false);
            modes::client::run(args).await
        }
        Command::Ctl(args) => ctl::run(args).await,
    }
}
