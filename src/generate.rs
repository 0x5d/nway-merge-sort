use rand::distributions::{Alphanumeric, DistString};
use rand::SeedableRng;
use std::io::{self, ErrorKind};
use std::sync::{mpsc, Arc};

use mpsc::Receiver;
use tokio::io::AsyncWriteExt;
use tokio::{fs::File, task::JoinSet};

use crate::bucket::{self, Bucket};

/// The value sent by workers to the writer when they have finished processing data.
const POISON_PILL: &str = "shutdown now";

/// Write size_bytes random data into file, using at most max_mem (RAM).
pub async fn generate_data(file: File, size_bytes: u64, max_mem: u64) -> io::Result<()> {
    let num_cores = std::thread::available_parallelism().unwrap().get();
    let mem_per_core = max_mem / num_cores as u64;
    let b = bucket::Bucket::new(num_cores as i32);
    let b = Arc::new(b);
    let writer_bucket = b.clone();
    let mut set = JoinSet::new();
    let (tx, rx) = mpsc::channel();

    let writer_handle = tokio::spawn(async move {
        writer(file, writer_bucket, rx, num_cores).await;
    });

    let mut remaining = size_bytes;

    while remaining > 0 {
        let to_write = if remaining < mem_per_core {
            remaining
        } else {
            mem_per_core
        };

        let tx = tx.clone();
        b.take();
        set.spawn_blocking(move || {
            let s = generate(to_write);
            if let Err(e) = tx.send(s) {
                eprintln!("{e}")
            }
        });

        remaining -= to_write;
    }

    while let Some(res) = set.join_next().await {
        let _ = res?;
    }

    for _ in 0..num_cores {
        tx.send(String::from(POISON_PILL))
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
    }

    writer_handle.await?;

    Ok(())
}

async fn writer(mut file: File, b: Arc<Bucket>, rx: Receiver<String>, n_threads: usize) {
    let mut shutdown_counter = 0;
    loop {
        if shutdown_counter == n_threads {
            return;
        }
        // TODO handle error.
        match rx.recv() {
            Ok(s) => {
                if s == POISON_PILL {
                    shutdown_counter += 1;
                    continue;
                }
                b.put();
                file.write_all(s.as_bytes()).await.unwrap();
            }
            Err(e) => {
                println!("{e}");
                return;
            }
        }
    }
}

fn generate(len: u64) -> String {
    Alphanumeric.sample_string(&mut rand::rngs::SmallRng::from_entropy(), len as usize)
}
