extern crate repo_stream;
use clap::Parser;
use repo_stream::drive::Processable;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Parser)]
struct Args {
    #[arg()]
    file: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
struct S(usize);

impl Processable for S {
    fn get_size(&self) -> usize {
        0 // no additional space taken, just its stack size (newtype is free)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { file } = Args::parse();
    let reader = tokio::fs::File::open(file).await?;
    let reader = tokio::io::BufReader::new(reader);

    let (commit, mut driver) =
        match repo_stream::drive::load_car(reader, |block| S(block.len()), 1024 * 1024).await? {
            repo_stream::drive::Vehicle::Lil(commit, mem_driver) => (commit, mem_driver),
            repo_stream::drive::Vehicle::Big(_) => panic!("can't handle big cars yet"),
        };

    log::info!("got commit: {commit:?}");

    let mut n = 0;
    while let Some(pairs) = driver.next_chunk(256).await? {
        n += pairs.len();
        // log::info!("got {rkey:?}");
    }
    log::info!("bye! {n}");

    Ok(())
}
