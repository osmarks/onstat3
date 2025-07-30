use axum::{response::{Html, IntoResponse, Response}, routing::get, Router, extract::State, body::Bytes, http::StatusCode};
use maud::{html, Markup, DOCTYPE};
use surge_ping::{PingIdentifier, PingSequence};
use std::{net::{IpAddr, SocketAddr}, sync::atomic::AtomicU16};
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use anyhow::Result;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use std::collections::VecDeque;
use histogram::Histogram;
use argh::FromArgs;

mod histogram;

#[derive(FromArgs)]
#[argh(description="LatencyStat network latency monitor")]
struct Args {
    #[argh(option, default="IpAddr::from_str(\"127.0.0.1\").unwrap()", description="IP address to ping")]
    address: IpAddr,
    #[argh(option, default="SocketAddr::from_str(\"[::]:7800\").unwrap()", description="listen address for webserver")]
    listen: SocketAddr,
    #[argh(option, default="1e6", description="minimum latency for visualization")]
    min_latency_ns: f64,
    #[argh(option, default="1e9", description="maximum latency for visualization")]
    max_latency_ns: f64
}

const HISTORY_SIZE: usize = 60 * 60 * 24 * 7;
const HISTORY_IMAGE_WIDTH: u32 = 60 * 10;
const HISTORY_IMAGE_HEIGHT: u32 = 6 * 24 * 7;

async fn do_requests(state: AppState) -> Result<()> {
    let mut interval = interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let mut socket = state.socket.lock().await;
        let time = match socket.ping(PingSequence(state.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)), &[]).await {
            Ok((_, time)) => time.as_nanos().try_into().unwrap(),
            Err(_e) => u64::MAX,
        };
        {
            // maintain average and counts over rolling window
            let mut history = state.history.write().await;
            let mut histogram = state.live_histogram.write().await;
            if history.times.len() >= HISTORY_SIZE {
                let old_entry = history.times.pop_front().unwrap();
                if old_entry != u64::MAX {
                    histogram.dec(old_entry as f64);
                    history.success_count -= 1;
                    history.total_time -= old_entry as u128;
                }
            }
            history.times.push_back(time);
            if time != u64::MAX {
                histogram.inc(time as f64);
                history.success_count += 1;
                history.total_time += time as u128;
            }
            // invalidate image
            state.image.write().await.clear();
        }
        drop(socket);
        interval.tick().await;
    }
}

struct History {
    times: VecDeque<u64>,
    total_time: u128,
    success_count: u64
}

#[derive(Clone)]
struct AppState {
    socket: Arc<Mutex<surge_ping::Pinger>>,
    image: Arc<RwLock<Bytes>>,
    live_histogram: Arc<RwLock<Histogram>>,
    history: Arc<RwLock<History>>,
    counter: Arc<AtomicU16>,
    config: Arc<Args>
}

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self.0)).into_response()
    }
}

impl<E> From<E> for AppError where E: Into<anyhow::Error> {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

fn blank_histogram(config: &Args) -> Histogram {
    Histogram::new(config.min_latency_ns, config.max_latency_ns, 1.05)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args: Args = argh::from_env();

    let socket = surge_ping::Client::new(&surge_ping::Config::builder().build())?;
    let mut socket = socket.pinger(args.address, PingIdentifier(0)).await;
    socket.timeout(Duration::from_secs(1));
    let socket = Arc::new(Mutex::new(socket));

    let image = Arc::new(RwLock::new(Bytes::new()));
    let live_histogram = blank_histogram(&args);
    let history = History {
        times: VecDeque::with_capacity(HISTORY_SIZE),
        total_time: 0,
        success_count: 0,
    };

    let args = Arc::new(args);

    let live_histogram = Arc::new(RwLock::new(live_histogram));
    let history = Arc::new(RwLock::new(history));

    let app_state = AppState { socket, image, live_histogram, history, counter: Arc::new(AtomicU16::new(0)), config: args.clone() };
    let app_state_ = app_state.clone();
    tokio::spawn(async move { do_requests(app_state_).await });

    let app = Router::new().route("/", get(handler)).route("/history.png", get(image_handler))
        .with_state(app_state);

    axum::Server::bind(&args.listen)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

const CSS: &'static str = "
body {
    font-family: sans-serif;
}

.title {
    margin-bottom: 0.5em;
}

h1, h2 {
    font-weight: normal;
    margin: 0;
}

h1 {
    border-bottom: 1px solid black;
}

.card {
    margin-bottom: 1em;
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
}

img {
    image-rendering: pixelated;
    -ms-interpolation-mode: nearest-neighbor;
    image-rendering: crisp-edges;
}

svg {
    shape-rendering: crispedges;
}
";

fn scale_latency(latency_ns: u64, config: &Args) -> f64 {
    let latency = latency_ns as f64;
    let raw_int = (latency.min(config.max_latency_ns).max(config.min_latency_ns).ln() - config.min_latency_ns.ln()) / (config.max_latency_ns.ln() - config.min_latency_ns.ln());
    raw_int * -0.8 + 1.0
}

async fn image_handler(State(state): State<AppState>) -> (StatusCode, Bytes) {
    let image = state.image.read().await;
    if image.len() > 0 {
        (StatusCode::OK, image.clone())
    } else {
        drop(image);
        let mut image = state.image.write().await;
        let mut buf = Vec::new();
        let mut encoder = png::Encoder::new(&mut buf, HISTORY_IMAGE_WIDTH, HISTORY_IMAGE_HEIGHT);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().unwrap();
        let history = state.history.read().await;

        let mut data = vec![0u8; HISTORY_IMAGE_WIDTH as usize * HISTORY_IMAGE_HEIGHT as usize * 3];
        for (i, time) in history.times.iter().enumerate() {
            data[i * 3 + 1] = (scale_latency(*time, &state.config) * 255.0) as u8;
        }

        writer.write_image_data(&data).unwrap();
        writer.finish().unwrap();
        *image = Bytes::from(buf);
        (StatusCode::OK, image.clone())
    }
}

async fn render(state: AppState) -> Markup {
    let padding = 40.0;
    let width = HISTORY_IMAGE_WIDTH as f64 - padding;
    let bars_height = 300 as f64;
    let histogram = state.live_histogram.read().await;
    let buckets: Vec<(f64, u64)> = histogram.buckets().collect();
    let first_nonzero_index = buckets.iter().position(|(_, c)| *c != 0);
    let first_nonzero_index_r = buckets.iter().rposition(|(_, c)| *c != 0);
    let buckets = buckets[first_nonzero_index.unwrap_or(0)..=first_nonzero_index_r.unwrap_or(buckets.len() - 1)].to_vec();
    let percentiles = vec![0.5, 0.9, 0.99, 0.999];
    let mut percentile_values = vec![];
    let total: u64 = buckets.iter().map(|(_, c)| *c).sum();
    let mut cumsum = 0;
    for (i, (bucket_max, count)) in buckets.iter().enumerate() {
        let bucket_start_perc = cumsum as f64 / total as f64;
        cumsum += count;
        let bucket_end_perc = cumsum as f64 / total as f64;
        for percentile in percentiles.iter().copied() {
            if percentile >= bucket_start_perc && percentile < bucket_end_perc {
                let ithresh = (percentile - bucket_start_perc) / (bucket_end_perc - bucket_start_perc);
                let ival = bucket_max / histogram.exp.powf(1.0 - ithresh);
                percentile_values.push((percentile, ithresh + i as f64, ival));
            }
        }
    }

    let history = state.history.read().await;
    let last_latency = history.times.iter().last().copied().unwrap_or(u64::MAX);
    let perc_up = history.success_count as f64 / history.times.len() as f64;
    let latency_ms = history.total_time as f64 / history.times.len() as f64 * 1e-6;

    let max_count = *buckets.iter().map(|(_i, x)| x).max().unwrap();
    let bar_width = width / buckets.len() as f64;
    let plot = html! {
        svg viewBox=(format!("{} 0 {} {}", -padding * 0.25, width + (padding * 0.75), bars_height + 50.0)) xmlns="http://www.w3.org/2000/svg" width=(format!("{}", width + padding)) height=(format!("{}", bars_height + 50.0)) {
            @for (i, (_max, count)) in buckets.into_iter().enumerate() {
                @let height = bars_height * (count as f64 / max_count as f64);
                rect width=(format!("{}", bar_width)) x=(format!("{}", bar_width * i as f64)) height=(format!("{}", height)) y=(format!("{}", bars_height - height)) {}
            }
            @for (i, (percentile, abs_position, value)) in percentile_values.into_iter().enumerate() {
                @let bottom_offset = if i % 2 == 0 { -10.0 } else { 10.0 };
                line x1=(format!("{}", abs_position * bar_width)) x2=(format!("{}", abs_position * bar_width)) y1="0" y2=(format!("{}", bars_height + 20.0 + bottom_offset )) stroke="green" {}
                text x=(format!("{}", abs_position * bar_width)) y=(format!("{}", bars_height + 30.0 + bottom_offset)) text-anchor="middle" style="font-size: 10px" { (format!("{:}%", percentile * 100.0)) }
                text x=(format!("{}", abs_position * bar_width)) y=(format!("{}", bars_height + 40.0 + bottom_offset)) text-anchor="middle" style="font-size: 10px" { (format!("{:.0}ms", value * 1e-6)) }
            }
        }
    };

    html! {
        div class="card" {
            div {
                div { (format!("{:.1}% up ({}/{}), {:.0}ms average latency", perc_up * 100., history.success_count, history.times.len(), latency_ms)) }
                @if last_latency != u64::MAX {
                    div { (format!("Last latency: {:.0}ms", last_latency as f64 * 1e-6)) }
                }
                (plot)
            }
            img src={"/history.png"};
        }
    }
}

async fn handler(State(state): State<AppState>) -> Result<Html<String>, AppError> {
    Ok(Html(html! {
        (DOCTYPE)
        meta charset="utf8";
        meta http-equiv="refresh" content="60";
        meta name="viewport" content="width=device-width, initial-scale=1";
        title { "LatencyStat" }
        style { (CSS) }
        body {
            h1 .title { "LatencyStat" }
            (render(state).await)
        }
    }.into_string()))
}
