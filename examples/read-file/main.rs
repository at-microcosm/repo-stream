extern crate repo_stream;
use clap::Parser;
use futures::TryStreamExt;
use iroh_car::CarReader;
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

    println!("hello!");

    let reader = CarReader::new(reader).await?;

    let root = reader
        .header()
        .roots()
        .first()
        .ok_or("missing root")?
        .clone();
    log::debug!("root: {root:?}");

    // let stream = Box::pin(reader.stream());
    let stream = std::pin::pin!(reader.stream());

    let (commit, v) = repo_stream::drive::Vehicle::init(&root, stream).await?;
    let mut record_stream = std::pin::pin!(v.stream());

    log::info!("got commit: {commit:?}");

    while let Some((rkey, rec)) = record_stream.try_next().await? {
        log::info!("got {rkey:?} {}", rec.len());
    }
    log::info!("bye!");

    Ok(())
}
