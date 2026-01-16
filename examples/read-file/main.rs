/*!
Read a CAR file with in-memory processing
*/

extern crate repo_stream;
use clap::Parser;
use repo_stream::{Driver, DriverBuilder, Output, Step};
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
        .with_block_processor(|block| block.len().to_ne_bytes().to_vec())
        .load_car(reader)
        .await?
    {
        Driver::Memory(commit, _, mem_driver) => (commit, mem_driver),
        Driver::Disk(_) => panic!("this example doesn't handle big CARs"),
    };

    log::info!("got commit: {commit:?}");

    while let Step::Value(records) = driver.next_chunk(256).await? {
        for Output { rkey, cid, data } in records {
            let size = usize::from_ne_bytes(data.try_into().unwrap());
            print!("0x");
            for byte in cid.to_bytes() {
                print!("{byte:>02x}");
            }
            println!(": {rkey} => record of len {}", size);
        }
    }

    Ok(())
}
