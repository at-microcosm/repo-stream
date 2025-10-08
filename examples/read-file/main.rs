extern crate repo_stream;
use clap::Parser;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Parser)]
struct Args {
    #[arg()] file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { file } = Args::parse();
    let reader = tokio::fs::File::open(file).await?;

    println!("hello!");
    repo_stream::drive::drive(reader).await?;

    Ok(())
}
