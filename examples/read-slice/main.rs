/*!
Read a CAR slice in memory and show some info about it.
*/

extern crate repo_stream;
use repo_stream::{DriverBuilder, LoadError, Output, WalkItem};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let reader = tokio::io::BufReader::new(tokio::io::stdin());

    let mut mem_car = match DriverBuilder::new()
        .with_block_processor(|block| block.len().to_ne_bytes().to_vec())
        .load_car(reader)
        .await
    {
        Ok(mc) => mc,
        Err(LoadError::MemoryLimitReached(_)) => panic!("this example doesn't handle big CARs"),
        Err(e) => return Err(e.into()),
    };

    println!(
        "\nthis slice is from {}, repo rev {}",
        mem_car.commit.did, mem_car.commit.rev
    );

    println!("included records:");

    let mut preceding: Option<String> = None;
    let mut trailing: Option<String> = None;
    let mut after_records = false;

    while let Some(items) = mem_car.next_chunk(256)? {
        for item in items {
            match item {
                WalkItem::Record(Output { cid, key, .. }) => {
                    after_records = true;
                    trailing = None;
                    print!("  SHA256 ");
                    for byte in cid.to_bytes().iter().skip(4).take(5) {
                        print!("{byte:02x}");
                    }
                    println!("...\t{key}");
                }
                WalkItem::MissingRecord { key, .. } => {
                    if !after_records {
                        preceding = Some(key);
                    } else if trailing.is_none() {
                        trailing = Some(key);
                    }
                }
                WalkItem::MissingSubtree { .. } | WalkItem::Node { .. } => {}
            }
        }
    }

    println!("done walking records present in the slice.");
    match preceding {
        Some(key) => println!("  -> key immediately before CAR slice: {key}"),
        None => println!(
            "  -> no key preceding the CAR slice, so it includes the leading edge of the tree."
        ),
    }
    match trailing {
        Some(key) => println!("  -> key immediately after CAR slice: {key}"),
        None => println!(
            "  -> no key following the CAR slice, so it includes the trailing edge of the tree."
        ),
    }

    Ok(())
}
