mod server;
mod player;

use player::{
    clean_old_output, 
    create_sink, 
    create_youtube,
    queue_worker,
    sink_poll,
    Queue,
    DownloadQueue,
    NowPlaying,
};
use server::create_server;
use tokio::sync::{
    Mutex, 
    mpsc,
};
use yt_dlp::Youtube;
use std::{
    collections::VecDeque,
    sync::Arc,
    path::PathBuf,
    env,
};

#[tokio::main]
async fn main() {

    let args: Vec<String> = env::args().collect();

    clean_old_output();

    let executables_dir = PathBuf::from("libs");
    let output_dir = PathBuf::from("output");

    let youtube: Arc<Youtube> = Arc::new(create_youtube(args, executables_dir.clone(), output_dir.clone()).await);

    let (queue_tx, queue_rx) = mpsc::channel(1);

    let queue: Queue = Arc::new(Mutex::new(VecDeque::new()));
    let download_queue: DownloadQueue = Arc::new(Mutex::new(VecDeque::new()));
    let playing: NowPlaying = Arc::new(Mutex::new(None));

    let (_stream, sink) = create_sink().await;
    let sink = Arc::new(Mutex::new(sink));
    
    tokio::spawn(
        queue_worker(
        queue_rx, 
        download_queue.clone(), 
        queue.clone(), 
        playing.clone(), 
        sink.clone(), 
        youtube.clone(), 
        output_dir.clone(), 
        executables_dir.clone()
        )
    );
    
    tokio::spawn(sink_poll(queue_tx.clone(), sink.clone()));

    create_server(queue.clone(), queue_tx.clone(), playing.clone(),sink.clone(), download_queue.clone()).await;
}