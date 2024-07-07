use std::{cmp::min, io, os::unix::fs::MetadataExt};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::Runtime,
    task::JoinSet,
};

// use crate::MAX_MEM;

pub async fn sort(r: Runtime, cfg: crate::Config) -> io::Result<()> {
    let file = File::open(&cfg.file).await?;
    let _res = split(r, file, &cfg.file).await?;
    Ok(())
}

async fn split(r: Runtime, mut file: File, filename: &str) -> io::Result<Vec<File>> {
    let meta = file.metadata().await?;
    let remainder = meta.size() % MAX_MEM;
    println!("size: {}", meta.size());
    println!("remainder: {remainder}");
    let no_intermediate_files = meta.size() / MAX_MEM + min(1, remainder);
    println!("{no_intermediate_files}");
    let mut intermediate_files = vec![];
    let mut set = JoinSet::new();
    println!("Allocating MAX_MEM ({MAX_MEM})");
    let mut buf = [0_u8; MAX_MEM as usize];
    for i in 0..no_intermediate_files {
        println!("{i}");
        let n = file.read(&mut buf).await?;
        let name = filename.to_owned().clone();
        let fut = async move {
            let name = format!("{}-int-{}.txt", name, i.to_string());

            let mut f = File::create(name).await.unwrap();
            f.write(&buf[..n]).await.unwrap();
            f
        };
        set.spawn_on(fut, r.handle());
    }
    while let Some(res) = set.join_next().await {
        let f = res?;
        intermediate_files.push(f);
        // println!("Joined writer.")
    }
    Ok(intermediate_files)
}
