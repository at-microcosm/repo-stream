/*!
Read a CAR file with in-memory processing
*/

extern crate repo_stream;
use clap::Parser;
use repo_stream::{Driver, DriverBuilder};
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

    let (commit, mut driver) = match DriverBuilder::new()
        .with_block_processor(|block| block.len())
        .load_car(reader)
        .await?
    {
        Driver::Memory(commit, mem_driver) => (commit, mem_driver),
        Driver::Disk(_) => panic!("this example doesn't handle big CARs"),
    };

    log::info!("got commit: {commit:?}");

    let mut n = 0;
    while let Some(pairs) = driver.next_chunk(256).await? {
        n += pairs.len();
        // log::info!("got {rkey:?}");
    }
    log::info!("bye! total records={n}");

    Ok(())
}
