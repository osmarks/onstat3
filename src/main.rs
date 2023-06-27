use axum::{response::Html, routing::get, Router, extract::{State, Path}, body::Bytes, http::StatusCode};
use maud::{Markup, html, DOCTYPE};
use std::{net::SocketAddr, collections::HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use sqlx::{Row, Sqlite, sqlite::{SqlitePoolOptions, SqliteConnectOptions, SqliteJournalMode}, Executor, Acquire, SqlitePool, pool::PoolConnection};
use anyhow::Result;
use std::str::FromStr;
use fnv::FnvHasher;
use int_enum::IntEnum;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::time::{interval, MissedTickBehavior};
use std::collections::VecDeque;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntEnum)]
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

async fn push_new_request(mut conn: PoolConnection<Sqlite>, site: &str, now: SystemTime, status: Status, latency_us: u64) -> Result<()> {
    let id = site_to_id(site);
    let (mut r_succ, mut r_req, r_latency, r_latency_sq, r_data_since) = sqlx::query("SELECT * FROM site WHERE id = ?")
        .bind(id)
        .map(|row| (row.get(1), row.get(2), row.get(3), row.get(4), row.get(5)))
        .fetch_optional(&mut conn).await?.unwrap_or((0i64, 0i64, [0; 16].to_vec(), [0; 16].to_vec(), 0i64));
    let mut r_latency = u128::from_le_bytes(r_latency.try_into().unwrap());
    let mut r_latency_sq = u128::from_le_bytes(r_latency_sq.try_into().unwrap());
    if r_data_since != 0 {
        let threshold: i64 = now.checked_sub(ROLLING_LEN).unwrap().duration_since(UNIX_EPOCH).unwrap().as_micros().try_into().unwrap();
        let reqs: Vec<(u8, i64)> = sqlx::query("SELECT status, latency_us FROM req WHERE timestamp >= ? AND timestamp < ? AND site = ?")
            .bind(r_data_since)
            .bind(threshold)
            .bind(id)
            .map(|row| (row.get(0), row.get(1)))
            .fetch_all(&mut conn).await?;
        for (status, latency_us) in reqs {
            let status = Status::from_int(status)?;
            r_req -= 1;
            if status == Status::Ok {
                r_succ -= 1;
                r_latency -= latency_us as u128;
                r_latency_sq -= (latency_us as u128) * (latency_us as u128);
            }
        }
    }
    r_req += 1;
    if status == Status::Ok {
        r_succ += 1;
        r_latency += latency_us as u128;
        r_latency_sq += (latency_us as u128) * (latency_us as u128);
    }
    let timestamp: i64 = now.duration_since(UNIX_EPOCH).unwrap().as_micros().try_into().unwrap();
    let mut tx = conn.begin().await?;
    sqlx::query("INSERT OR REPLACE INTO site VALUES (?, ?, ?, ?, ?, ?)")
        .bind(id).bind(r_succ).bind(r_req)
        .bind(&r_latency.to_le_bytes()[..]).bind(&r_latency_sq.to_le_bytes()[..])
        .bind(timestamp)
        .execute(&mut tx)
        .await?;
    sqlx::query("INSERT INTO req (site, timestamp, status, latency_us) VALUES (?, ?, ?, ?)")
        .bind(id).bind(timestamp).bind(status.int_value()).bind(latency_us as i64)
        .execute(&mut tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn do_request(pool: &SqlitePool, site: &str) -> Result<(Status, i64)> {
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
    push_new_request(conn, site, end, status, latency_us).await?;
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

async fn do_requests(pool: Arc<SqlitePool>, sites: Arc<[String]>, images: Arc<RwLock<HashMap<u32, Vec<u8>>>>) {
    for site in sites.iter() {
        let pool = pool.clone();
        let site = site.to_string();
        let images = images.clone();
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
            loop {
                interval.tick().await;
                match do_request(&*pool, &site).await {
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

struct Site {
    url: String,
    successful_requests: u64,
    requests: u64,
    total_latency_us: u128,
    total_latency_sq_us: u128,
    last_latency_us: u64,
    last_status: Status
}

fn render_site(site: Site) -> Markup {
    let latency = site.total_latency_us / (site.successful_requests as u128);
    let perc_up = (site.successful_requests as f64) / (site.requests as f64);
    let variance = (site.total_latency_sq_us / (site.successful_requests as u128)) - (latency * latency);
    let stdev = (variance as f64).sqrt() / 1000.;
    let latency_ms = latency / 1000;
    let (status_class, status_icon, status_text) = match site.last_status {
        Status::Ok => ("ok", "✓", format!("Latency {}ms", site.last_latency_us / 1000)),
        Status::FetchError => ("fetch-error", "⚠", "HTTP error".to_string()),
        Status::HttpError => ("http-error", "✕", "Fetch failed".to_string()),
        Status::Timeout => ("timeout", "✕", "Timed out".to_string())
    };
    html! {
        div class={"card " (status_class) } {
            div .left {
                h2 { (status_icon) " " (site.url) }
                div {
                    (status_text)
                }
                div { (format!("{:.1}% up ({}/{})", perc_up * 100., site.successful_requests, site.requests)) ", " (latency_ms) "ms latency in last week, standard deviation " (format!("{:.1}", stdev)) "ms" }
            }
            img src={"/image/" (site_to_id(&site.url))};
        }
    }
}

#[derive(Clone)]
struct AppState {
    pool: Arc<SqlitePool>,
    sites: Arc<[String]>,
    images: Arc<RwLock<HashMap<u32, Vec<u8>>>>
}

#[tokio::main]
async fn main() -> Result<()> {
    let sites: Arc<[String]> = std::env::args().skip(1).collect();

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(
            SqliteConnectOptions::from_str("sqlite://./onstat.sqlite3")?
                .journal_mode(SqliteJournalMode::Wal)
                .create_if_missing(true)).await?;
    let pool = Arc::new(pool);

    println!("{:?}", sites);
    let pool_ = pool.clone();
    let sites_ = sites.clone();
    let images = Arc::new(RwLock::new(HashMap::new()));
    let images_ = images.clone();
    tokio::spawn(async move { do_requests(pool_, sites_, images_).await });

    pool.execute("
    CREATE TABLE IF NOT EXISTS site (
        id INTEGER PRIMARY KEY,
        running_successes INTEGER NOT NULL,
        running_requests INTEGER NOT NULL,
        running_latency_us BLOB NOT NULL,
        running_latency_squared_us BLOB NOT NULL,
        running_data_since_timestamp INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS req (
        id INTEGER PRIMARY KEY,
        site INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        status INTEGER NOT NULL,
        latency_us INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS req_ts_idx ON req (timestamp);").await?;

    let app = Router::new().route("/", get(handler)).route("/image/:id", get(image_handler))
        .with_state(AppState { pool, sites, images });

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
}";

async fn image_handler(Path(id): Path<u32>, State(AppState { pool: _, sites: _, images }): State<AppState>) -> (StatusCode, Bytes) {
    match images.read().await.get(&id) { 
        Some(data) => (StatusCode::OK, Bytes::from(data.clone())),
        None => (StatusCode::NOT_FOUND, Bytes::from("Not Found"))
    }
}

async fn handler(State(AppState { pool, sites: site_urls, images: _ }): State<AppState>) -> Html<String> {
    let mut conn = pool.acquire().await.unwrap();
    let mut sites = vec![];
    let mut sites_up = 0;
    for site in site_urls.iter() {
        let id = site_to_id(site);
        let (r_succ, r_req, r_latency, r_latency_sq, _r_data_since) = sqlx::query("SELECT * FROM site WHERE id = ?")
            .bind(id)
            .map(|row| (row.get(1), row.get(2), row.get(3), row.get(4), row.get(5)))
            .fetch_optional(&mut *conn).await.unwrap().unwrap_or((0i64, 0i64, [0; 16].to_vec(), [0; 16].to_vec(), 0i64));
        let r_latency = u128::from_le_bytes(r_latency.try_into().unwrap());
        let r_latency_sq = u128::from_le_bytes(r_latency_sq.try_into().unwrap());
        let (last_status, last_latency): (u8, i64) = sqlx::query("SELECT status, latency_us FROM req WHERE site = ? ORDER BY timestamp DESC")
            .bind(id)
            .map(|row| (row.get(0), row.get(1)))
            .fetch_one(&mut conn).await.unwrap();
        let last_status = Status::from_int(last_status).unwrap();
        if last_status == Status::Ok {
            sites_up += 1;
        }
        sites.push(Site {
            url: site.to_string(),
            last_latency_us: last_latency as u64,
            last_status,
            successful_requests: r_succ as u64,
            requests: r_req as u64,
            total_latency_sq_us: r_latency_sq,
            total_latency_us: r_latency
        });
    }
    Html(html! {
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
    }.into_string())
}