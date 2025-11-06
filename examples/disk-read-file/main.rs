extern crate repo_stream;
use clap::Parser;
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

impl Processable for S {
    fn get_size(&self) -> usize {
        0 // no additional space taken, just its stack size (newtype is free)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { car, tmpfile } = Args::parse();
    let reader = tokio::fs::File::open(car).await?;
    let reader = tokio::io::BufReader::new(reader);

    // let kb = 2_usize.pow(10);
    let mb = 2_usize.pow(20);

    let limit_mb = 32;

    let mut driver =
        match repo_stream::drive::load_car(reader, |block| S(block.len()), 10 * mb).await? {
            repo_stream::drive::Vehicle::Lil(_, _) => panic!("try this on a bigger car"),
            repo_stream::drive::Vehicle::Big(big_stuff) => {
                let disk_store = repo_stream::disk::SqliteStore::new(tmpfile.clone(), limit_mb);
                let (commit, driver) = big_stuff.finish_loading(disk_store).await?;
                log::warn!("big: {:?}", commit);
                driver
            }
        };

    println!("hello!");

    let mut n = 0;
    loop {
        let (d, p) = driver.next_chunk(1024).await?;
        driver = d;
        let Some(pairs) = p else {
            break;
        };
        n += pairs.len();
        // log::info!("got {rkey:?}");
    }
    // log::info!("now is the time to check mem...");
    // tokio::time::sleep(std::time::Duration::from_secs(22)).await;
    drop(driver);
    log::info!("bye! {n}");

    std::fs::remove_file(tmpfile).unwrap(); // need to also remove -shm -wal

    Ok(())
}
