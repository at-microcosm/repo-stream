/*!
Read a CAR file with in-memory processing
*/

extern crate repo_stream;
use clap::Parser;
use repo_stream::{DriverBuilder, Output, Step};
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Parser)]
struct Args {
    #[arg()]
    file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { file } = Args::parse();
    let reader = tokio::fs::File::open(file).await?;
    let reader = tokio::io::BufReader::new(reader);

    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(1000)
        .with_block_processor(|block| block.len().to_ne_bytes().to_vec())
        .load_car(reader)
        .await?;

    log::info!("got commit: {:?}", mem_car.commit);

    while let Step::Value(records) = mem_car.next_chunk(256)? {
        for Output {
            key: _,
            cid: _,
            data: _,
        } in records
        {
            // process records
        }
    }

    Ok(())
}
