/*!
Read a CAR file by spilling to disk
*/

extern crate repo_stream;
use clap::Parser;
use repo_stream::{DiskStore, Driver, process::noop};
use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Args {
    #[arg()]
    car: PathBuf,
    #[arg()]
    tmpfile: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let Args { car, tmpfile } = Args::parse();

    // repo-stream takes an AsyncRead as input. wrapping a filesystem read in
    // BufReader can provide a really significant performance win.
    let reader = tokio::fs::File::open(car).await?;
    let reader = tokio::io::BufReader::new(reader);

    // configure how much memory can be used before spilling to disk.
    // real memory usage may differ somewhat.
    let in_mem_limit = 10; // MiB

    // configure how much memory sqlite is allowed to use when dumping to disk
    let db_cache_mb = 32; // MiB

    log::info!("hello! reading the car...");

    // in this example we only bother handling CARs that are too big for memory
    // `noop` helper means: do no block processing, store the raw blocks
    let driver = match Driver::load_car(reader, noop, in_mem_limit).await? {
        Driver::Memory(_, _) => panic!("try this on a bigger car"),
        Driver::Disk(big_stuff) => {
            // we reach here if the repo was too big and needs to be spilled to
            // disk to continue

            // set up a disk store we can spill to
            let disk_store = DiskStore::new(tmpfile.clone(), db_cache_mb).await?;

            // do the spilling, get back a (similar) driver
            let (commit, driver) = big_stuff.finish_loading(disk_store).await?;

            // at this point you might want to fetch the account's signing key
            // via the DID from the commit, and then verify the signature.
            log::warn!("big's comit: {:?}", commit);

            // pop the driver back out to get some code indentation relief
            driver
        }
    };

    // collect some random stats about the blocks
    let mut n = 0;
    let mut zeros = 0;

    log::info!("walking...");

    // this example uses the disk driver's channel mode: the tree walking is
    // spawned onto a blocking thread, and we get chunks of rkey+blocks back
    let (mut rx, join) = driver.to_channel(512);
    while let Some(r) = rx.recv().await {
        let pairs = r?;

        // keep a count of the total number of blocks seen
        n += pairs.len();

        for (_, block) in pairs {
            // for each block, count how many bytes are equal to '0'
            // (this is just an example, you probably want to do something more
            // interesting)
            zeros += block.into_iter().filter(|&b| b == b'0').count()
        }
    }

    log::info!("arrived! joining rx...");

    // clean up the database. would be nice to do this in drop so it happens
    // automatically, but some blocking work happens, so that's not allowed in
    // async rust. 🤷‍♀️
    join.await?.reset_store().await?;

    log::info!("done. n={n} zeros={zeros}");

    Ok(())
}
