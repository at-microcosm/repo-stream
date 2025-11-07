extern crate repo_stream;
use clap::Parser;
use repo_stream::{Driver, noop};
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

    let driver = match Driver::load_car(reader, noop, 10 * mb).await? {
        Driver::Lil(_, _) => panic!("try this on a bigger car"),
        Driver::Big(big_stuff) => {
            let disk_store = repo_stream::disk::SqliteStore::new(tmpfile.clone(), limit_mb).await?;
            let (commit, driver) = big_stuff.finish_loading(disk_store).await?;
            log::warn!("big: {:?}", commit);
            driver
        }
    };

    let mut n = 0;
    let mut zeros = 0;
    let mut rx = driver.to_channel(512);

    log::debug!("walking...");
    while let Some(r) = rx.recv().await {
        let pairs = r?;
        n += pairs.len();
        for (_, block) in pairs {
            zeros += block.into_iter().filter(|&b| b == b'0').count()
        }
    }
    log::debug!("done walking!");

    // log::info!("now is the time to check mem...");
    // tokio::time::sleep(std::time::Duration::from_secs(22)).await;
    log::info!("bye! n={n} zeros={zeros}");

    std::fs::remove_file(tmpfile).unwrap(); // need to also remove -shm -wal

    Ok(())
}
