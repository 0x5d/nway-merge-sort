use std::{
    collections::BinaryHeap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    sync::Arc,
};

use tokio::task::JoinSet;

use crate::{bucket, Config, BLOCK_SIZE};

struct Block {
    fileIdx: i64,
    block: [u8; BLOCK_SIZE],
}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        todo!()
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}

pub async fn sort(cfg: crate::Config) -> io::Result<()> {
    println!("Split");
    let mut files = split(&cfg).await?;
    println!("Sort");
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&cfg.file)?;

    let mut heap = BinaryHeap::new();
    let mut buf = [0 as u8; crate::BLOCK_SIZE as usize];
    println!("Sort loop");
    loop {
        THE PROBLEM HERE IS THAT I'M POPPING 1 BLOCK, BUT READING N BLOCKS IN THE NEXT ITERATION.
        I gotta fill it back with a block coming from the same file from which the popped block came.

        let mut keep = vec![];
        for mut f in &files {
            // println!("Seeking");
            f.seek(io::SeekFrom::Start(0))?;
            // println!("Reading from int file");
            let n = f.read(&mut buf)?;
            // println!("Read from int file");
            keep.push(n != 0); // We'll keep only files which still have contents to read.
            heap.push(buf.clone());
        }
        match heap.pop() {
            Some(b) => {
                // println!("Writing to target file");
                let n = file.write(&b)?;
                // println!("Wrote {n} bytes");
            } //write b to file,
            None => break, // The heap is empty - we're done.
        }
        if !files.is_empty() {
            let mut iter = keep.iter();
            files.retain(|_| *iter.next().unwrap());
            if files.is_empty() {
                break;
            }
        }
    }
    Ok(())
}

async fn split(cfg: &Config) -> io::Result<Vec<File>> {
    let file = File::open(&cfg.file)?;
    let meta = file.metadata()?;
    println!("size: {}", meta.size());
    let num_cores = std::thread::available_parallelism().unwrap().get();
    let no_intermediate_files = meta.size() / cfg.int_file_size;
    println!("Intermediate files {no_intermediate_files}");
    let b = bucket::Bucket::new(num_cores as i32);
    let b = Arc::new(b);
    let mut set = JoinSet::new();
    for i in 0..no_intermediate_files {
        let b = b.clone();
        let name = cfg.file.to_owned().clone();
        let int_file_dir = cfg.int_file_dir.clone();
        let int_file_size = cfg.int_file_size;
        set.spawn_blocking(move || {
            fs::create_dir_all(&int_file_dir)?;
            let filename = format!("{}/{}.txt", int_file_dir, i.to_string());
            let mut f = OpenOptions::new()
                .write(true)
                .read(true)
                .truncate(true)
                .open(filename)?;
            let mut file = File::open(name.clone())?;

            let o = i as u64 * int_file_size;
            let offset = io::SeekFrom::Start(o);
            file.seek(offset)?;

            let mut buf = vec![0 as u8; int_file_size as usize];
            b.take();
            // TODO: What if bytes_read < int_file_size? E.g. if the source file doesn't align with
            // int_file_size.
            let bytes_read = file.read(buf.as_mut())?;
            let blocks_per_file = buf.len() / crate::BLOCK_SIZE;
            let mut blocks = Vec::with_capacity(blocks_per_file);
            for i in 0..blocks_per_file {
                // TODO: is buf.take(crate::BLOCK_SIZE) better?
                let offset = i * crate::BLOCK_SIZE;
                blocks.push(&buf[offset..offset + crate::BLOCK_SIZE]);
            }
            blocks.sort_unstable();

            // TODO: check written bytes match the expected val.
            match f.write(&blocks.concat()) {
                Ok(_) => {
                    b.put();
                    Ok(f)
                }
                Err(e) => Err(e),
            }
        });
    }
    let mut files = Vec::with_capacity(no_intermediate_files as usize);
    while let Some(res) = set.join_next().await {
        let f = res??;
        files.push(f);
    }
    Ok(files)
}
