use rodio::{Decoder, OutputStream, Sink};
use std::{
    collections::VecDeque, 
    env::consts::EXE_SUFFIX, 
    fs::{
        self, 
        read_dir, 
        remove_dir_all, 
        remove_file, 
        File,
    }, 
    io::BufReader, 
    path::PathBuf, 
    process::Command, 
    sync::Arc, 
    time::Duration,
};
use yt_dlp::{
    fetcher::{
        deps::{
            Libraries, 
            LibraryInstaller
        },
        download_manager::ManagerConfig,
    }, 
    Youtube
};
use tokio::{
    sync::{mpsc, Mutex}, 
    time
};
use serde::{Deserialize, Serialize};
use reqwest;
use scraper::{Html, Selector};

const MAX_LOADS: u32 = 5;
const MAX_BUFFER_SIZE: usize = 2e7 as usize;

pub type NowPlaying = Arc<Mutex<Option<DownloadedSong>>>;
pub type Queue = Arc<Mutex<VecDeque<Song>>>;
pub type DownloadQueue = Arc<Mutex<VecDeque<DownloadedSong>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Song {
    pub link: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadedSong {
    link: String,
    pub title: Option<String>,
    raw_file_path: Option<PathBuf>,
    mp3_file_path: Option<PathBuf>,

}

pub enum QueueCommand {
    Empty,
    Add,
}

pub fn new_download_queue() -> DownloadQueue {
    Arc::new(Mutex::new(VecDeque::new())) 
}

pub fn new_queue() -> Queue {
    Arc::new(Mutex::new(VecDeque::new()))
}

pub fn new_now_playing() -> NowPlaying {
    Arc::new(Mutex::new(None))
}

async fn create_youtube_from_existing(executables_dir: PathBuf, output_dir: PathBuf, update: bool) -> Youtube {
    let dlp = executables_dir.join(format!("yt-dlp{}", EXE_SUFFIX));
    let ffmpeg = executables_dir.join(format!("ffmpeg{}",EXE_SUFFIX));

    let libraries = Libraries::new(dlp, ffmpeg);

    let download_manager_config = ManagerConfig {
        max_buffer_size: MAX_BUFFER_SIZE,
        ..Default::default()
    };

    let youtube = Youtube::with_download_manager_config(libraries, output_dir, download_manager_config).expect("Unable to create youtube with existing libs.");

    if update {
        println!("Libraries found and created.\nPeforming update procedure.");    
        match youtube.update_downloader().await {
            Ok(()) => {
                println!("Successfully updated Libraries");
            },
            Err(e) => {
                println!("Error updating libraries.\nError: {}", e);
            }
        }
    }    
    youtube
}

async fn reset_binaries(executables_dir: PathBuf) {
    match read_dir(&executables_dir) {
        Ok(contents) => {
            contents.filter_map(Result::ok).for_each(|entry| {
                let path = entry.path();
                let result = if path.is_dir() {
                    remove_dir_all(&path)
                } else {
                    remove_file(&path)
                };
                if let Err(e) = result {
                    eprintln!("Failed to remove: {}\nError: {}", path.display(), e);
                }
            });
        },
        Err(e) => {
            println!("Error reading directory with current binaries.\nError: {}", e);
        }
    }

    let installer = LibraryInstaller::new(executables_dir.clone());

    match installer.install_youtube(None).await {
        Ok(path) => {
            println!("Installed a new yt-dlp to {}", path.display());
        },
        Err(e) => {
            println!("Fatal error installing yt-dlp.\nError: {}", e);
            panic!()
        }
    }

    match installer.install_ffmpeg(None).await {
        Ok(path) => {
            println!("Installed a new ffmpeg to {}", path.display());
        },
        Err(e) => {
            println!("Fatal error installing ffmpeg.\nError: {}", e);
            panic!()
        }
    }

}

pub async fn create_youtube(args: Vec<String>, executables_dir: PathBuf, output_dir: PathBuf) -> Youtube {

    let is_fresh: bool;

    match read_dir(&executables_dir) {
        Ok(mut files) => {
            if files.next().is_none() {
                is_fresh = true;
            } else {
                is_fresh = false;
            }
        },
        Err(e) => {
            println!("Error checking library install status.\nError: {}\nInstalling new libraries.", e);
            is_fresh = true;
        }
    }
    
    let youtube: Youtube;
    
    if args.contains(&"--reset".to_string()) || is_fresh {
        reset_binaries(executables_dir.clone()).await;
        youtube = create_youtube_from_existing(executables_dir.clone(), output_dir.clone(), false).await;
    } else {
        youtube = create_youtube_from_existing(executables_dir.clone(), output_dir.clone(), true).await;
    }

    youtube
}

pub fn clean_old_output() {
    let old_output = PathBuf::from("output");
    
    if old_output.exists() {
        match fs::remove_dir_all(old_output) {
            Ok(()) => {
                println!("Successfully removed old output files.");
            },
            Err(e) => {
                println!("Unable to delete old output files. Behavior may be unexpected.\nError: {}",e);
            }
        }
    }
}

async fn downloader(
    youtube: Arc<Youtube>,
    output_dir: PathBuf,
    song_link: Song,
    counter: &u32,
 ) -> Result<(PathBuf, String), Box<dyn std::error::Error + Send + Sync>> {

    //let youtube = youtube.clone();
    let file_path = format!("track_{}_raw.webm",&counter);
    let url = song_link.link;

    println!("Starting Download for link: {}", &url);

    youtube
        .download_audio_stream_with_quality(
            &url, 
            &file_path, 
            yt_dlp::model::AudioQuality::Best, 
            yt_dlp::model::AudioCodecPreference::Any
        ).await?;

    println!("Downloaded Link: {} \nTo: {}", &url, &file_path);

    let file_path = output_dir.join(file_path);

    Ok((file_path, url))

}

async fn converter(
    file_path: PathBuf,
    url: String,
    output_dir: PathBuf,
    executables_dir: PathBuf,
    counter: &u32,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {

    let mp3_path = output_dir.join(format!("track_{}.mp3",&counter));

    println!("Starting conversion of:\nLink: {}\nFile Path: {}", &url, &file_path.display());

    let _status = Command::new(executables_dir.join("ffmpeg"))
        .args([
            "-y",
            "-i",
            &file_path.to_str().unwrap(),
            "-vn",
            "-codec:a",
            "libmp3lame",
            "-qscale:a",
            "2",
            &mp3_path.to_str().unwrap(),
        ])
        .status()?;

    println!("Converted: {}\nWith URL: {}\nTo: {}", 
        &file_path.to_str().unwrap(), 
        &url, 
        &mp3_path.to_str().unwrap()
    );

    Ok(mp3_path)
}

async fn get_title(
    song: Song,
) -> Option<String> {
    let link = song.clone().link;

    match reqwest::get(&link).await {
        Ok(body) => {
            match body.text().await {
                Ok(text) => {
                    let document = Html::parse_document(&text);
                    match Selector::parse(r#"meta[property="og:title"]"#) {
                        Ok(selector) => {
                            if let Some(tag) = document.select(&selector).next() {
                                if let Some(content) = tag.value().attr("content") {
                                    return Some(content.to_string())
                                } else {
                                    return None
                                }
                            } else {
                                return None
                            }
                        },
                        Err(e) => {
                            println!("Error parsing web document from link: {}\nError: {}", &link, e);
                            return None
                        }
                    }
                },
                Err(e) => {
                    println!("Error extracting text from webpage with link: {}\nError: {}", &link, e);
                    return None
                }
            }
        },
        Err(e) => {
            println!("Error getting response body for title acquisition for link: {}\nError: {}", &link, e);
            return None
        }
    }
}

pub async fn create_sink() -> (OutputStream, Sink) {
    let (stream, stream_handle) = OutputStream::try_default()
        .expect("Failed to pickup a stream handle");

    println!("Output Stream Created");

    let sink = Sink::try_new(&stream_handle).expect("Failed to create sink");

    println!("Sink Created");

    (stream, sink)
}

pub async fn sink_poll(
    queue_tx: mpsc::Sender<QueueCommand>, 
    sink: Arc<Mutex<Sink>>,
) {
    
    loop {
        if sink.clone().lock().await.empty() {

            match queue_tx.send(QueueCommand::Empty).await {
                Ok(()) => {
                    println!("Sent QueueCommand From SinkPoller: Empty");
                },
                Err(e) => {
                    println!("Error sending command to queue.\n Error: {}", e);
                }
            };
            
            time::sleep(Duration::from_secs(10)).await;

        } else {
            time::sleep(Duration::from_secs(5)).await;
        }
    }
}

async fn add_to_download_queue(
    queue: Queue,
    downloaded_queue: DownloadQueue,
    youtube: Arc<Youtube>,
    output_dir: PathBuf,
    executables_dir: PathBuf,
    counter: &mut u32,

) {
    if let Some(next) = queue.clone().lock().await.pop_front() {
        match downloader(youtube.clone(), output_dir.clone(), next.clone(), &counter).await {
            Ok((raw_path, url)) => {
                match converter(raw_path.clone(), url.clone(), output_dir.clone(), executables_dir.clone(), &counter).await {
                    Ok(mp3_path) => {

                        let title = get_title(next).await;

                        let next_queued: DownloadedSong = DownloadedSong { 
                            link: (url), 
                            title: title,
                            raw_file_path: (Some(raw_path)), 
                            mp3_file_path: (Some(mp3_path)) 
                        };

                        if (*counter as u32) < MAX_LOADS {
                            *counter += 1;
                        } else {
                            *counter = 0;
                        }

                        downloaded_queue.clone().lock().await.push_back(next_queued);
                    },
                    Err(e) => {
                        println!("Error Converting\n Error: {}", e);
                    }
                }
            },
            Err(e) => {
                println!("Error Downloading Next Song. \nError: {}\nSong: {}", e, next.link);
            }
        }
    }
}

async fn get_next_downloaded_song(
    downloaded_queue: DownloadQueue
) -> Option<DownloadedSong> {

    if let Some(next_up) = downloaded_queue.clone().lock().await.pop_front() {
            Some(next_up)
        } else {
            None
        }

}

async fn cycle_player(
    next_up: DownloadedSong,
    sink: Arc<Mutex<Sink>>,
    now_playing: NowPlaying,
) {
    if let Some(path) = next_up.clone().mp3_file_path {
        match File::open(&path) {
            Ok(file) => {
                match Decoder::new(BufReader::new(file)) {
                    Ok(source) => {
                        sink.clone().lock().await.append(source);
                        let mut now_playing_guard = now_playing.lock().await;
                        *now_playing_guard = Some(next_up);
                    },
                    Err(e) => {
                        println!("Error Decoding File:\nError: {}\nFile: {}",e, &path.display());
                    }
                }
            },
            Err(e) => {
                println!("Error Opening File\n{}",e)
            }
        }
    }
}

pub async fn queue_worker(
    mut queue_rx: mpsc::Receiver<QueueCommand>,
    downloaded_queue: DownloadQueue,
    queue: Queue,
    now_playing: NowPlaying,
    sink: Arc<Mutex<Sink>>,
    youtube: Arc<Youtube>,
    output_dir: PathBuf,
    executables_dir: PathBuf,
) {

    let mut counter: u32 = 0;

    while let Some(command) = queue_rx.recv().await {
        match command {
            QueueCommand::Add => {
                println!("Received QueueCommand: Add");

                if queue.clone().lock().await.len() > 0 && downloaded_queue.clone().lock().await.len() < (MAX_LOADS as usize) + 1 {
                    add_to_download_queue(queue.clone(), 
                            downloaded_queue.clone(), 
                            youtube.clone(), 
                            output_dir.clone(), 
                            executables_dir.clone(), 
                            &mut counter)
                            .await;
                }
            },
            QueueCommand::Empty => {
                println!("Received QueueCommand: Empty");
                
                if let Some(next_up) =  get_next_downloaded_song(downloaded_queue.clone()).await {
                    
                    cycle_player(next_up, 
                        sink.clone(), 
                        now_playing.clone())
                        .await;
                    
                    add_to_download_queue(queue.clone(), 
                        downloaded_queue.clone(), 
                        youtube.clone(), 
                        output_dir.clone(), 
                        executables_dir.clone(), 
                        &mut counter)
                        .await;

                    }
            }
        }
    }
}