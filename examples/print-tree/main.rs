/*!
Read a CAR slice in memory and show some info about it.
*/

extern crate repo_stream;
use repo_stream::{Driver, DriverBuilder};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let reader = tokio::io::BufReader::new(tokio::io::stdin());

    let (commit, driver) = match DriverBuilder::new()
        .with_block_processor(|block| block.len().to_ne_bytes().to_vec())
        .load_car(reader)
        .await?
    {
        Driver::Memory(commit, _, mem_driver) => (commit, mem_driver),
        Driver::Disk(_) => panic!("this example doesn't handle big CARs"),
    };

    println!(
        "\nthis slice is from {}, repo rev {}\n\n",
        commit.did, commit.rev
    );

    driver.viz(commit.data)?;

    Ok(())
}
