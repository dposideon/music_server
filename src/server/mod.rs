use crate::player::{DownloadQueue, Song, Queue, QueueCommand, NowPlaying};

use std::{
    fs,
    path::Path,
    sync::Arc
};
use axum::{
    Router,
    Json,
    extract::State,
    routing::{get, post},
    response::{Html, IntoResponse},
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc},
};
use tower_http::services::ServeDir;
use local_ip_address::local_ip;
use rodio::Sink;
use qrcode:: QrCode;
use image::Luma;

#[derive(Clone)]
pub struct AppState {
    pub queue: Queue,
    index_html: Arc<String>,
    queue_tx: mpsc::Sender<QueueCommand>,
    playing: NowPlaying,
    sink: Arc<Mutex<Sink>>,
    downloaded_queue: DownloadQueue,
}

pub async fn create_server(queue: Queue, queue_tx: mpsc::Sender<QueueCommand>, playing: NowPlaying, sink: Arc<Mutex<Sink>>, downloaded_queue: DownloadQueue) {
    let index_html = Arc::new(tokio::fs::read_to_string("static/index3.html").await.unwrap());
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

    let port = listener.local_addr().expect("could not get listener address").port();
    let ip = local_ip().expect("Error for local ip address");
    println!("Listening on http://{}:{}\n", &ip, &port);

    let destination = format!("http://{}:{}", &ip, &port);

    generate_qr_code(destination);


    let state = AppState {
        queue,
        index_html,
        queue_tx,
        playing,
        sink,
        downloaded_queue,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/api/queue", get(get_queue).post(add_to_queue))
        .route("/api/play", post(play))
        .route("/api/pause", post(pause))
        .route("/api/skip", post(skip))
        .route("/api/now_playing", get(now_playing))
        .route("/api/downloaded_queue", get(get_downloaded_queue))
        .nest_service("/static", ServeDir::new("static"))
        .with_state(state)
        .fallback(handler_404);

    axum::serve(listener, app.into_make_service()).await.unwrap();
}

async fn index(State(state): State<AppState>) -> impl IntoResponse {
    println!("{:<12} - Handler_Home -", "HANDLER");

    let content = state.index_html.clone().to_string();

    Html(content)
}

async fn get_queue(State(state): State<AppState>) -> impl IntoResponse {
    println!("{:<12} - Handler_Get_Queue -", "HANDLER");

    let queue = state.queue.lock().await;
    Json(queue.clone())
}

async fn get_downloaded_queue(
    State(state): State<AppState>
) -> impl IntoResponse {
    println!("{:<12} - Handler_Get_Downloaded_Queue -", "HANDLER");
    let q = state.downloaded_queue.lock().await;
    Json(q.clone())
}

async fn add_to_queue(
    State(state): State<AppState>,
    Json(payload): Json<Song>) 
    -> impl IntoResponse {
    println!("{:<12} - Handler_Add_To_Queue -", "HANDLER");

    let mut queue = state.queue.lock().await;
    queue.push_back(payload);

    match state.queue_tx.send(QueueCommand::Add).await {
        Ok(()) => {
            println!("Sent queue command (Add) from server");
        },
        Err(e) => {
            println!("Error sending queue command from server.\nError: {}", e);
        }
    }
}

async fn pause(
    State(state): State<AppState>
) {
    println!("{:<12} - Handler_Pause -", "HANDLER");
    state.sink.clone().lock().await.pause();
}

async fn skip(
    State(state): State<AppState>
) {
    println!("{:<12} - Handler_Skip -", "HANDLER");
    state.sink.clone().lock().await.skip_one();
}

async fn play(
    State(state): State<AppState>
) {
    println!("{:<12} - Handler_Play -", "HANDLER");
    state.sink.clone().lock().await.play();
}

async fn now_playing(State(state): State<AppState>) -> impl IntoResponse {
    println!("{:<12} - Handler_Now_Playing -", "HANDLER");
    let now = state.playing.lock().await.clone();
    Json(now)
}

async fn handler_404() -> impl IntoResponse {
    println!("{:<12} - Handler_404 -", "HANDLER");
    let fallback_page = fs::read_to_string("static/404.html").unwrap();
    
    (
        axum::http::StatusCode::NOT_FOUND,
        Html(fallback_page)
    )
}

fn generate_qr_code(
    destination: String,
) {
    match QrCode::new(&destination.as_bytes()) {
        Ok(code) => {
            let image = code.render::<Luma<u8>>().build();
            let output_path = Path::new("static/qr.png");

            match image.save(output_path) {
                Ok(()) => {
                    println!("Successfully created QR code redirecting to {}", &destination);
                },
                Err(e) => {
                    println!("Error saving QR image.\nError: {}", e)
                }
            }
        },
        Err(e) => {
            println!("Error generating new QR code.\nError: {}", e)
        }
    }
}