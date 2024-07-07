use clap::{command, Parser};
use std::{
    io::{self, ErrorKind},
    process,
};
use tokio::{fs::File, runtime};

mod generate;
mod sort;

const BLOCK_SIZE: u64 = 4096;
const MAX_MEM: u64 = 536870912; // 0.5GiB

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// Generate & sort big files.
struct Config {
    /// If set, will generate a file of the given size at the given path.
    #[arg(short, long, default_value_t = false)]
    generate: bool,
    /// If set, will sort the file at the given path.
    #[arg(long, default_value_t = false)]
    sort: bool,
    /// The filepath.
    #[arg(short, long)]
    file: String,
    /// The size of the file to generate.
    #[arg(short, long)]
    size: Option<u64>,
    //// The size of the file to generate.
    // #[arg(short, long, default_value_t = 536870912)]
    // max_mem: u64,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cfg = Config::parse();
    assert!(
        MAX_MEM > BLOCK_SIZE,
        "Max allowed memory must be larger than {BLOCK_SIZE}B"
    );
    if cfg.generate {
        let res = match cfg.size {
            None => Result::Err(io::Error::new(
                ErrorKind::InvalidInput,
                "If --generate is chosen, --size must be set.",
            )),
            Some(s) => {
                let file = File::create(cfg.file.clone()).await?;
                generate::generate_data(file, s).await
            }
        };
        match res {
            Err(e) => {
                eprintln!("{e}");
                process::exit(1);
            }
            Ok(_) => {
                println!("File generated at {}", cfg.file);
                return Ok(());
            }
        }
    } else if cfg.sort {
        // let r = runtime::Builder::new_multi_thread()
        //     .enable_io()
        //     // .thread_stack_size(MAX_MEM as usize * 2)
        //     .build()?;
        // sort::sort(r, cfg).await?;
    } else {
        eprintln!("One of --generate or --sort must be passed.");
        process::exit(1);
    }
    Ok(())
}
