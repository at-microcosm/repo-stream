extern crate repo_stream;
use clap::Parser;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Parser)]
struct Args {
    #[arg()]
    car: PathBuf,
    #[arg()]
    tmpfile: PathBuf,
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

    let driver = match repo_stream::drive::load_car(reader, |block| block.len(), 10 * mb).await? {
        repo_stream::drive::Vehicle::Lil(_, _) => panic!("try this on a bigger car"),
        repo_stream::drive::Vehicle::Big(big_stuff) => {
            let disk_store = repo_stream::disk::SqliteStore::new(tmpfile.clone(), limit_mb).await?;
            let (commit, driver) = big_stuff.finish_loading(disk_store).await?;
            log::warn!("big: {:?}", commit);
            driver
        }
    };

    let mut n = 0;
    let (mut rx, worker) = driver.rx(512).await?;

    log::debug!("walking...");
    while let Some(pairs) = rx.recv().await {
        n += pairs.len();
    }
    log::debug!("done walking! joining...");

    worker.await.unwrap().unwrap();

    log::debug!("joined.");

    // log::info!("now is the time to check mem...");
    // tokio::time::sleep(std::time::Duration::from_secs(22)).await;
    log::info!("bye! {n}");

    std::fs::remove_file(tmpfile).unwrap(); // need to also remove -shm -wal

    Ok(())
}
