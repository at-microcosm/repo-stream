extern crate repo_stream;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tokio::io::AsyncRead;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::BTreeMap;
use clap::Parser;
use repo_stream::DriverBuilder;
use std::path::PathBuf;


#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("drive error: {0}")]
    DriveError(#[from] repo_stream::DriveError),
    #[error("send error: {0}")]
    SendError(String),
    #[error("failed to die")]
    FailedToDie,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
struct Args {
    #[arg()]
    folder: PathBuf,
}

async fn get_cars(
    cars_folder: PathBuf,
    tx: async_channel::Sender<(tokio::io::BufReader<tokio::fs::File>, String)>,
) -> Result<()> {
    let mut dir = tokio::fs::read_dir(cars_folder).await?;
    while let Some(entry) = dir.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let reader = tokio::fs::File::open(&entry.path()).await?;
        let reader = tokio::io::BufReader::new(reader);
        tx.send((reader, entry.file_name().to_string_lossy().into())).await.map_err(|e| Error::SendError(e.to_string()))?;
    }
    Ok(())
}

async fn counter<R: AsyncRead + Unpin + Send + Sync + 'static>(
    car_rx: async_channel::Receiver<(R, String)>,
    totals: Arc<Mutex<BTreeMap<usize, usize>>>,
    n: Arc<AtomicUsize>,
) -> Result<()> {

    let builder = DriverBuilder::new()
        .with_block_processor(|_| vec![]);

    while let Ok((f, name)) = car_rx.recv().await {
        n.fetch_add(1, Ordering::Relaxed);

        let Ok(Some(counts)) = builder
            .clone()
            .count_entries(f)
            .await
            .inspect_err(|e| eprintln!("{name} failed: {e}"))
        else {
            continue
        };

        let mut t = totals.lock().await;
        for (entries, n) in counts {
            *t.entry(entries).or_default() += n;
        }
        drop(t);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { folder } = Args::parse();

    let mut set = JoinSet::<Result<()>>::new();

    tokio::fs::create_dir_all(folder.clone()).await?;

    let (cars_tx, cars_rx) = async_channel::bounded(2);
    set.spawn(get_cars(folder, cars_tx));

    let n: Arc<AtomicUsize> = Arc::new(0.into());

    let totals = Arc::new(Mutex::new(BTreeMap::new()));

    for _ in 0..15 {
        set.spawn(counter(cars_rx.clone(), totals.clone(), n.clone()));
    }
    drop(cars_rx);

    let (die, mut til_death) = tokio::sync::oneshot::channel();
    let monitor = n.clone();
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = &mut til_death => break,
            }
            eprintln!("repos: {}", monitor.load(Ordering::Relaxed));
        }
    });

    while let Some(res) = set.join_next().await {
        println!("task from set joined: {res:?}");
    }
    die.send(()).map_err(|_| Error::FailedToDie)?;

    println!("repos: {}", n.load(Ordering::SeqCst));
    for (n, c) in totals.lock().await.iter() {
        println!("{n}\t{c}");
    }

    Ok(())
}
