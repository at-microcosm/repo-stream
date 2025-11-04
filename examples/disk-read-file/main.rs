extern crate repo_stream;
use clap::Parser;
use repo_stream::disk::RedbStore;
use repo_stream::drive::Processable;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Parser)]
struct Args {
    #[arg()]
    car: PathBuf,
    #[arg()]
    tmpfile: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
struct S(usize);

impl Processable for S {}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { car, tmpfile } = Args::parse();
    let reader = tokio::fs::File::open(car).await?;
    let reader = tokio::io::BufReader::new(reader);

    let mut driver =
        match repo_stream::drive::load_car(reader, |block| S(block.len()), 1024).await? {
            repo_stream::drive::Vehicle::Lil(_, _) => panic!("try this on a bigger car"),
            repo_stream::drive::Vehicle::Big(big_stuff) => {
                let disk_store = RedbStore::new(tmpfile);
                let (commit, driver) = big_stuff.finish_loading(disk_store).await?;
                log::warn!("big: {:?}", commit);
                driver
            }
        };

    println!("hello!");

    let mut n = 0;
    loop {
        let (d, Some(pairs)) = driver.next_chunk(256).await? else {
            break;
        };
        driver = d;
        n += pairs.len();
        // log::info!("got {rkey:?}");
    }
    log::info!("bye! {n}");

    Ok(())
}
