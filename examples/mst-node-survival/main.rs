extern crate repo_stream;
use repo_stream::link::ThingKind;
use clap::Parser;
use repo_stream::{Driver, DriverBuilder};
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Parser)]
struct Args {
    #[arg()]
    a: PathBuf,
    // #[arg()]
    // b: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { a } = Args::parse();
    let reader_a = tokio::fs::File::open(a.clone()).await?;
    let reader_a = tokio::io::BufReader::new(reader_a);

    // let reader_b = tokio::fs::File::open(b.clone()).await?;
    // let reader_b = tokio::io::BufReader::new(reader_b);

    let builder = DriverBuilder::new()
        .with_mem_limit_mb(1000)
        .with_block_processor(|_| vec![]);

    log::info!("loading {a:?}...");
    let driver_a = match builder.clone().load_car(reader_a).await? {
        Driver::Memory(_, _, mem_driver) => mem_driver,
        Driver::Disk(_) => panic!("this example doesn't handle big CARs"),
    };

    // log::info!("loading {b:?}...");
    // let driver_b = match builder.load_car(reader_b).await? {
    //     Driver::Memory(_, _, mem_driver) => mem_driver,
    //     Driver::Disk(_) => panic!("this example doesn't handle big CARs"),
    // };

    let mut total_referenced_nodes = 0;
    let mut total_l0_nodes = 0;
    let mut total_referenced_records = 0;
    let mut total_low_records = 0;
    // let mut records_a = 0;
    // let mut layer0_a = 0;

    for (_link, mpb) in driver_a.blocks {
        // // skips (probably nodes)
        // if !mpb.unknown_depth() {
        //     continue;
        // }

        // // records
        // if mpb.to_node().is_some() {
        //     continue;
        // }


        // nodes
        let Some(node) = mpb.to_node() else {
            continue;
        };
        let Some(depth) = node.depth else {
            continue;
        };
        for thing in node.things {
            match thing.kind {
                ThingKind::Record(_) => {
                    total_referenced_records += 1;
                    if depth <= 1 {
                        total_low_records += 1;
                    }
                }
                ThingKind::ChildNode => {
                    total_referenced_nodes += 1;
                    if depth == 1 {
                        total_l0_nodes += 1;
                    }
                }
            }
        }

        // // levels
        // let Some(node) = mpb.to_node() else {
        //     continue;
        // };
        // if node.depth != Some(1) {
        //     continue;
        // }

        // total_a += 1;

        // if driver_b.blocks.contains_key(&link) {
        //     surviving_a += 1;
        // }
    }

    eprintln!("referenced nodes: {total_referenced_nodes}");
    eprintln!("referenced records: {total_referenced_records}");
    let total_links = total_referenced_nodes + total_referenced_records;
    eprintln!("total links: {}", total_links);

    eprintln!("layer 0+1 records: {total_low_records}");
    eprintln!("low recs of records: {:.1}",
        100. * f64::try_from(total_low_records).unwrap() / f64::try_from(total_referenced_records).unwrap()
    );

    eprintln!("layer 0 nodes: {total_l0_nodes}");
    eprintln!("low nodes of nodes: {:.1}",
        100. * f64::try_from(total_l0_nodes).unwrap() / f64::try_from(total_referenced_nodes).unwrap()
    );

    let low_links = total_l0_nodes + total_low_records;
    eprintln!("low links of all links: {:.1}",
        100. * f64::try_from(low_links).unwrap() / f64::try_from(total_links).unwrap()
    );

    Ok(())
}
