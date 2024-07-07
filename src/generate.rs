use rand::distributions::{Alphanumeric, DistString};
use rand::SeedableRng;
use std::io;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task::{JoinError, JoinHandle};

use mpsc::Receiver;
use tokio::task::JoinSet;

pub async fn generate_data(file: File, size_bytes: u64) -> io::Result<()> {
    let mut remaining = size_bytes;
    let num_cores = std::thread::available_parallelism().unwrap().get();
    let mem_per_core = crate::MAX_MEM / num_cores as u64;

    println!("Cores: {num_cores}");

    let pool = GeneratorPool::new(num_cores, file);

    while remaining > 0 {
        let to_write = if remaining < mem_per_core {
            remaining
        } else {
            mem_per_core
        };

        pool.generate(to_write as usize).unwrap();

        remaining -= to_write;
    }

    pool.close().await?;
    Ok(())
}

struct GeneratorPool {
    set: JoinSet<std::result::Result<(), JoinError>>,
    writer: JoinHandle<()>,
    tx: Sender<Option<usize>>,
}

impl GeneratorPool {
    fn new(size: usize, mut file: File) -> GeneratorPool {
        let (tx, rx) = mpsc::channel();
        let (w_tx, w_rx) = mpsc::channel::<String>();
        let writer = tokio::spawn(async move {
            loop {
                // TODO handle error.
                match w_rx.recv() {
                    Ok(s) => file.write_all(s.as_bytes()).await.unwrap(),
                    Err(e) => {
                        println!("{e}");
                        return;
                    }
                }
            }
        });
        let arc = Arc::new(Mutex::new(rx));
        let ws = (0..size).map(|_| Worker::new(arc.clone(), w_tx.clone()).handle);
        let set = JoinSet::from_iter(ws);
        GeneratorPool { set, writer, tx }
    }

    fn generate(&self, len: usize) -> std::result::Result<(), mpsc::SendError<Option<usize>>> {
        self.tx.send(Some(len))
    }

    async fn close(self) -> io::Result<()> {
        // Send a None to signal shutdown.
        (0..self.set.len()).for_each(|_| self.tx.send(None).expect("yikes"));

        let mut set = self.set;
        // Join all the workers.
        while let Some(res) = set.join_next().await {
            let _ = res?;
        }
        self.writer.await?;
        Ok(())
    }
}

struct Worker {
    handle: JoinHandle<()>,
}

impl Worker {
    fn new(rx: Arc<Mutex<Receiver<Option<usize>>>>, w_tx: Sender<String>) -> Worker {
        let handle = tokio::task::spawn_blocking(move || {
            loop {
                // println!("waiting for more work");
                match rx.lock().unwrap().recv() {
                    Ok(Some(n)) => {
                        let s = Alphanumeric
                            .sample_string(&mut rand::rngs::SmallRng::from_entropy(), n);
                        if let Err(e) = w_tx.send(s) {
                            eprintln!("Got an error on writer sender: {e}, shutting down.");
                            break;
                        }
                    }
                    _ => break,
                }
            }
        });
        Worker { handle }
    }
}
