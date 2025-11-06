mod cli;

use anyhow::Result;
use clap::Parser;

fn main() -> Result<()> {
    let cli = cli::Cli::parse();
    let config = cli.into_config();
    ulp_merge::run(config)
}
