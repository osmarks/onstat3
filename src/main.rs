use axum::{response::{Html, IntoResponse, Response}, routing::get, Router, extract::{State, Path}, body::Bytes, http::StatusCode};
use maud::{html, Markup, PreEscaped, DOCTYPE};
use std::{net::SocketAddr, collections::HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use sqlx::{Row, Sqlite, sqlite::{SqlitePoolOptions, SqliteConnectOptions, SqliteJournalMode}, Executor, Acquire, SqlitePool, pool::PoolConnection, SqliteExecutor};
use anyhow::Result;
use std::str::FromStr;
use fnv::FnvHasher;
use int_enum::IntEnum;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::time::{interval, MissedTickBehavior};
use std::collections::VecDeque;
use serde::{Serialize, Deserialize};
use futures::TryStreamExt;
use histogram::Histogram;

mod histogram;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntEnum, Serialize, Deserialize)]
enum Status {
    Ok = 0,
    HttpError = 1,
    Timeout = 2,
    FetchError = 3
}

fn site_to_id(s: &str) -> u32 {
    let mut hasher: FnvHasher = Default::default();
    s.hash(&mut hasher);
    hasher.finish() as u32
}

const ROLLING_LEN: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const MAX_TIME: Duration = Duration::from_secs(15);
const INTERVAL: Duration = Duration::from_secs(30);
const HISTORY_IMAGE_SAMPLES: usize = (12 * 7 * 24 * 60 * 60) / 30;
const HISTORY_IMAGE_WIDTH: u32 = 120 * 6;
const HISTORY_IMAGE_HEIGHT: u32 = 168 * 2;

fn get_rolling_threshold(now: SystemTime) -> i64 {
    now.checked_sub(ROLLING_LEN).unwrap().duration_since(UNIX_EPOCH).unwrap().as_micros().try_into().unwrap()
}

async fn push_new_request(mut conn: PoolConnection<Sqlite>, site: &str, now: SystemTime, status: Status, latency_us: u64, histogram: &RwLock<Histogram>) -> Result<()> {
    let id = site_to_id(site);
    let mut site = read_site(&mut conn, id).await?;
    let threshold: i64 = get_rolling_threshold(now);
    let mut histogram = histogram.write().await;
    if site.last_threshold != 0 {
        let reqs: Vec<(u8, i64)> = sqlx::query("SELECT status, latency_us FROM req WHERE timestamp >= ? AND timestamp < ? AND site = ?")
            .bind(site.last_threshold)
            .bind(threshold)
            .bind(id)
            .map(|row| (row.get(0), row.get(1)))
            .fetch_all(&mut conn).await?;
        for (status, latency_us) in reqs {
            let status = Status::from_int(status)?;
            site.requests -= 1;
            if status == Status::Ok {
                site.successful_requests -= 1;
                site.total_latency_us -= latency_us as u128;
                site.total_latency_sq_us -= (latency_us as u128) * (latency_us as u128);
                histogram.dec(latency_us as f64);
            }
        }
    }
    site.last_threshold = threshold;
    site.requests += 1;
    site.last_latency_us = latency_us;
    site.last_status = status;
    if status == Status::Ok {
        site.successful_requests += 1;
        site.total_latency_us += latency_us as u128;
        site.total_latency_sq_us += (latency_us as u128) * (latency_us as u128);
        histogram.inc(latency_us as f64);
    }
    let timestamp: i64 = now.duration_since(UNIX_EPOCH).unwrap().as_micros().try_into().unwrap();
    let mut tx = conn.begin().await?;
    site.histogram = histogram.clone();
    write_site(&mut tx, site).await?;
    sqlx::query("INSERT INTO req (site, timestamp, status, latency_us) VALUES (?, ?, ?, ?)")
        .bind(id).bind(timestamp).bind(status.int_value()).bind(latency_us as i64)
        .execute(&mut tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn do_request(pool: &SqlitePool, site: &str, histogram: &RwLock<Histogram>) -> Result<(Status, i64)> {
    let client = reqwest::Client::builder().timeout(MAX_TIME).redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let start = SystemTime::now();
    let status = match client.get(site)
        .header("User-Agent", "onstat3")
        .send()
        .await {
            Ok(res) if res.status().is_success() || res.status().is_redirection() => Status::Ok,
            Ok(_res) => Status::HttpError,
            Err(e) if e.is_status() => Status::HttpError,
            Err(e) if e.is_timeout() => Status::Timeout,
            Err(_) => Status::FetchError
        };
    let end = SystemTime::now();
    let latency_us = end.duration_since(start).unwrap().as_micros().try_into().unwrap();
    let conn = pool.acquire().await?;
    push_new_request(conn, site, end, status, latency_us, histogram).await?;
    Ok((status, latency_us as i64))
}

fn push_color(v: &mut VecDeque<u8>, (r, g, b): (u8, u8, u8)) {
    v.push_front(b);
    v.push_front(g);
    v.push_front(r);
}

pub const MIN_LATENCY: f64 = 5000.0;
pub const MAX_LATENCY: f64 = 15000000.0;

fn scale_latency(latency_us: i64) -> f64 {
    let latency = latency_us as f64;
    let raw_int = (latency.min(MAX_LATENCY).max(MIN_LATENCY).ln() - MIN_LATENCY.ln()) / (MAX_LATENCY.ln() - MIN_LATENCY.ln());
    raw_int * -0.8 + 1.0
}

fn generate_image(data: &mut VecDeque<u8>) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut encoder = png::Encoder::new(&mut buf, HISTORY_IMAGE_WIDTH, HISTORY_IMAGE_HEIGHT);
    encoder.set_color(png::ColorType::Rgb);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().unwrap();
    writer.write_image_data(data.make_contiguous()).unwrap();
    writer.finish().unwrap();
    buf
}

fn status_and_latency_to_color(status: Status, latency_us: i64) -> (u8, u8, u8) {
    match status {
        Status::HttpError => (0xFF, 0x7F, 0),
        Status::Timeout => (0, 0, 0),
        Status::FetchError => (0xFF, 0, 0),
        Status::Ok => (0, (255. * scale_latency(latency_us)) as u8, 0)
    }
}

async fn do_requests(state: AppState) {
    for site in state.sites.iter() {
        let pool = state.pool.clone();
        let site = site.to_string();
        let state = state.clone();
        let images = state.images.clone();
        tokio::spawn(async move {
            let mut history_image_buffer: VecDeque<u8> = VecDeque::with_capacity(HISTORY_IMAGE_SAMPLES * 3);
            for _ in 0..HISTORY_IMAGE_SAMPLES {
                push_color(&mut history_image_buffer, (0x7E, 0x1E, 0x9C));
            }
            let history_end = SystemTime::now();
            let history_start = history_end.checked_sub((HISTORY_IMAGE_SAMPLES as u32) * INTERVAL).unwrap().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
            let history_end = history_end.duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
            let interval_us = INTERVAL.as_micros() as i64;
            let historical_latencies: Vec<(i64, u8, i64)> = sqlx::query("SELECT timestamp, status, latency_us FROM req WHERE site = ? AND timestamp > ? ORDER BY timestamp DESC")
                .bind(site_to_id(&site))
                .bind(history_start)
                .map(|row| (row.get(0), row.get(1), row.get(2)))
                .fetch_all(&mut pool.acquire().await.unwrap()).await.unwrap();
            for (timestamp, status, latency_us) in historical_latencies {
                let pix: usize = ((history_end - timestamp) / interval_us).try_into().unwrap();
                let (r, g, b) = status_and_latency_to_color(Status::from_int(status).unwrap(), latency_us);
                history_image_buffer[3 * pix] = r;
                history_image_buffer[3 * pix + 1] = g;
                history_image_buffer[3 * pix + 2] = b;
            }
            images.write().await.insert(site_to_id(&site), generate_image(&mut history_image_buffer));
            let mut interval = interval(INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let histogram = &state.live_histograms[&site_to_id(&site)];
            loop {
                interval.tick().await;
                match do_request(&*pool, &site, histogram).await {
                    Ok((status, latency_us)) => {
                        for _ in 0..3 { history_image_buffer.pop_back(); }
                        push_color(&mut history_image_buffer, status_and_latency_to_color(status, latency_us));
                        images.write().await.insert(site_to_id(&site), generate_image(&mut history_image_buffer));
                    },
                    Err(e) => eprintln!("{} {:?}", site, e)
                }
            }
        });
    }
}

#[derive(Serialize, Deserialize)]
struct Site {
    url: String,
    successful_requests: u64,
    requests: u64,
    total_latency_us: u128,
    total_latency_sq_us: u128,
    last_latency_us: u64,
    last_threshold: i64,
    last_status: Status,
    histogram: Histogram
}

fn render_site(site: Site) -> Markup {
    let latency_us = site.total_latency_us / (site.successful_requests as u128);
    let perc_up = (site.successful_requests as f64) / (site.requests as f64);
    let latency_ms = latency_us / 1000;
    let (status_class, status_icon, status_text) = match site.last_status {
        Status::Ok => ("ok", "✓", format!("Latency {}ms", site.last_latency_us / 1000)),
        Status::FetchError => ("fetch-error", "⚠", "HTTP error".to_string()),
        Status::HttpError => ("http-error", "✕", "Fetch failed".to_string()),
        Status::Timeout => ("timeout", "✕", "Timed out".to_string())
    };

    let padding = 40.0;
    let width = HISTORY_IMAGE_WIDTH as f64 - padding;
    let bars_height = 300 as f64;
    let buckets: Vec<(f64, u64)> = site.histogram.buckets().collect();
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
                println!("{}", ithresh);
                let ival = bucket_max / site.histogram.exp.powf(1.0 - ithresh);
                percentile_values.push((percentile, ithresh + i as f64, ival));
            }
        }
    }
    println!("{:?}", percentile_values);
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
                text x=(format!("{}", abs_position * bar_width)) y=(format!("{}", bars_height + 40.0 + bottom_offset)) text-anchor="middle" style="font-size: 10px" { (format!("{:.0}ms", value / 1000.0)) }
            }
        }
    };

    html! {
        div class={"card " (status_class) } {
            div .left {
                h2 { (status_icon) " " (site.url) }
                div {
                    (status_text)
                }
                div { (format!("{:.1}% up ({}/{})", perc_up * 100., site.successful_requests, site.requests)) ", " (latency_ms) "ms latency in last week" }
                (plot)
            }
            img src={"/image/" (site_to_id(&site.url)) ".png"};
        }
    }
}

#[derive(Clone)]
struct AppState {
    pool: Arc<SqlitePool>,
    sites: Arc<[String]>,
    images: Arc<RwLock<HashMap<u32, Vec<u8>>>>,
    live_histograms: Arc<HashMap<u32, RwLock<Histogram>>>
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

fn blank_histogram() -> Histogram {
    Histogram::new(MIN_LATENCY, MAX_LATENCY, 1.05)
}

#[tokio::main]
async fn main() -> Result<()> {
    let sites: Arc<[String]> = std::env::args().skip(1).collect();

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(
            SqliteConnectOptions::from_str("sqlite://./onstat.sqlite3")?
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(9999))
                .create_if_missing(true)).await?;
    let pool = Arc::new(pool);

    println!("{:?}", sites);
    let sites_ = sites.clone();
    let images = Arc::new(RwLock::new(HashMap::new()));
    let mut live_histograms = HashMap::new();

    pool.execute("
    CREATE TABLE IF NOT EXISTS site (
        id INTEGER PRIMARY KEY,
        running_successes INTEGER NOT NULL,
        running_requests INTEGER NOT NULL,
        running_latency_us BLOB NOT NULL,
        running_latency_squared_us BLOB NOT NULL,
        running_data_since_timestamp INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS site_v2 (
        id INTEGER PRIMARY KEY,
        data BLOB NOT NULL
    );
    CREATE TABLE IF NOT EXISTS req (
        id INTEGER PRIMARY KEY,
        site INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        status INTEGER NOT NULL,
        latency_us INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS req_ts_idx ON req (timestamp);").await?;

    for site in sites.iter() {
        let id = site_to_id(site);
        let has_ported = sqlx::query("SELECT * FROM site_v2 WHERE id = ?").bind(id).fetch_optional(&*pool).await?.is_some();
        if !has_ported {
            let mut histogram = blank_histogram();
            let threshold = get_rolling_threshold(SystemTime::now());
            let mut requests = 0;
            let mut last_latency_us = 0;
            let mut last_status = Status::Ok;
            let mut successful_requests = 0;
            let mut total_latency_us = 0;
            let mut total_latency_sq_us = 0;
            let mut rows = sqlx::query("SELECT latency_us, status FROM req WHERE site = ? AND timestamp >= ? ORDER BY timestamp ASC").bind(id).bind(threshold).fetch(&*pool);
            while let Some(row) = rows.try_next().await? {
                last_latency_us = row.get::<i64, usize>(0) as u64;
                last_status = Status::from_int(row.get(1)).unwrap();
                requests += 1;
                if last_status == Status::Ok {
                    successful_requests += 1;
                    total_latency_us += last_latency_us as u128;
                    total_latency_sq_us += (last_latency_us as u128) * (last_latency_us as u128);
                    histogram.inc(last_latency_us as f64);
                }
            }
            let site = Site {
                successful_requests,
                requests,
                total_latency_us,
                total_latency_sq_us,
                last_threshold: threshold,
                url: sites.iter().find(|s| site_to_id(*s) == id).unwrap().clone(),
                last_latency_us,
                last_status,
                histogram
            };
            write_site(&mut pool.acquire().await?, site).await?;
        }
    }

    for site in &*sites_ {
        let id = site_to_id(site);
        let site = read_site(&*pool, id).await?;
        live_histograms.insert(id, RwLock::new(site.histogram));
    }

    let live_histograms = Arc::new(live_histograms);

    let app_state = AppState { pool, sites, images, live_histograms };
    let app_state_ = app_state.clone();
    tokio::spawn(async move { do_requests(app_state_).await });

    let app = Router::new().route("/", get(handler)).route("/image/:id", get(image_handler))
        .with_state(app_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 7800));
    println!("listening on {}", addr);
    axum::Server::bind(&addr)
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

.card.ok h2 {
    color: green;
}
.card.http-error h2 {
    color: orange;
}
.card.fetch-error h2 {
    color: red;
}
.card.timeout h2 {
    color: red;
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

async fn image_handler(Path(id): Path<String>, State(AppState { live_histograms: _, pool: _, sites: _, images }): State<AppState>) -> (StatusCode, Bytes) {
    let id = id.split_once(".").unwrap_or((&id, "")).0;
    let id = match u32::from_str(id) {
        Ok(id) => id,
        Err(_) => return (StatusCode::NOT_FOUND, Bytes::from("Not Found"))
    };
    match images.read().await.get(&id) { 
        Some(data) => (StatusCode::OK, Bytes::from(data.clone())),
        None => (StatusCode::NOT_FOUND, Bytes::from("Not Found"))
    }
}

async fn read_site<'a, E: SqliteExecutor<'a>>(conn: E, id: u32) -> Result<Site> {
    let row = sqlx::query("SELECT data FROM site_v2 WHERE id = ?").bind(id).fetch_one(conn).await?;
    let row: Vec<u8> = row.get(0);
    let site = rmp_serde::from_slice(&row)?;
    Ok(site)
}

async fn write_site<'a, E: SqliteExecutor<'a>>(conn: E, data: Site) -> Result<()> {
    sqlx::query("INSERT OR REPLACE INTO site_v2 VALUES (?, ?)").bind(site_to_id(&data.url)).bind(rmp_serde::to_vec_named(&data).unwrap()).execute(conn).await?;
    Ok(())
}

async fn handler(State(AppState { live_histograms, pool, sites: site_urls, images: _ }): State<AppState>) -> Result<Html<String>, AppError> {
    let histograms = live_histograms;
    for (id, histo) in histograms.iter() {
        println!("{} {:?}", id, histo.read().await);
    }
    let mut conn = pool.acquire().await?;
    let mut sites = vec![];
    let mut sites_up = 0;
    for site in site_urls.iter() {
        let id = site_to_id(site);
        let site = read_site(&mut conn, id).await?;
        if site.last_status == Status::Ok {
            sites_up += 1;
        }
        sites.push(site);
    }
    Ok(Html(html! {
        (DOCTYPE)
        meta charset="utf8";
        meta http-equiv="refresh" content="60";
        meta name="viewport" content="width=device-width, initial-scale=1";
        title { (sites_up) "/" (sites.len()) " up - OnStat" }
        style { (CSS) }
        body {
            h1 .title { "OnStat" }
            h2 .title { (sites_up) "/" (sites.len()) " up" }
            @for site in sites {
                (render_site(site))
            }
        }
    }.into_string()))
}