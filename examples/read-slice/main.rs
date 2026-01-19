/*!
Read a CAR slice in memory and show some info about it.
*/

extern crate repo_stream;
use repo_stream::{Driver, DriverBuilder, Output, Step};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let reader = tokio::io::BufReader::new(tokio::io::stdin());

    let (commit, prev_rkey, mut driver) = match DriverBuilder::new()
        .with_block_processor(|block| block.len().to_ne_bytes().to_vec())
        .load_car(reader)
        .await?
    {
        Driver::Memory(commit, prev, mem_driver) => (commit, prev, mem_driver),
        Driver::Disk(_) => panic!("this example doesn't handle big CARs"),
    };

    println!(
        "\nthis slice is from {}, repo rev {}",
        commit.did, commit.rev
    );
    if let Some(rkey) = prev_rkey {
        println!("  -> key immediately before CAR slice: {rkey}");
    } else {
        println!(
            "  -> no key preceeding the CAR slice, so it includes the leading edge of the tree."
        );
    }

    println!("included records:");
    let end = loop {
        match driver.next_chunk(256).await? {
            Step::Value(chunk) => {
                for Output { cid, rkey, .. } in chunk {
                    print!("  SHA256 ");
                    for byte in cid.to_bytes().iter().skip(4).take(5) {
                        print!("{byte:02x}");
                    }
                    println!("...\t{rkey}");
                }
            }
            Step::End(e) => break e,
        }
    };

    println!("done walking records present in the slice.");
    if let Some(rkey) = end {
        println!("  -> key immediately after CAR slice: {rkey}");
    } else {
        println!(
            "  -> no key proceeding the CAR slice, so it includes the trailing edge of the tree."
        );
    }

    Ok(())
}
