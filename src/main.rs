use bb8::{Pool, PooledConnection};
use bb8_redis::RedisConnectionManager;
use redis::AsyncCommands;
use tokio::time::Duration;

type RedisPool = Pool<RedisConnectionManager>;
type RedisConnection = redis::aio::Connection;
type Redis<'a> = PooledConnection<'a, RedisConnectionManager>;

enum DBStrategy<'a> {
    CheckoutEveryTime(i64, RedisPool),
    KeepConnection(i64, RedisConnection),
    KeepConnectionFromPool(i64, &'a RedisPool, Redis<'a>),
}

impl<'a> DBStrategy<'a> {
    fn _count(&self) -> i64 {
        match self {
            Self::CheckoutEveryTime(state, _) => *state,
            Self::KeepConnection(state, _) => *state,
            Self::KeepConnectionFromPool(state, _, _) => *state,
        }
    }

    async fn do_a_write(&mut self) {
        match self {
            Self::CheckoutEveryTime(state, pool) => {
                let mut conn = pool.get().await.unwrap();
                let x: i64 = conn.incr("counta", 1).await.unwrap();
                *state = x;
            }
            Self::KeepConnection(state, conn) => {
                let x: i64 = conn.incr("counta", 1).await.unwrap();
                *state = x;
            }
            Self::KeepConnectionFromPool(state, _, conn) => {
                let x: i64 = conn.incr("counta", 1).await.unwrap();
                *state = x;
            }
        }
    }

    async fn do_a_read(&mut self) {
        match self {
            Self::CheckoutEveryTime(state, pool) => {
                let mut conn = pool.get().await.unwrap();
                let x: Option<i64> = conn.get("counta").await.unwrap();
                if let Some(x) = x {
                    *state = x;
                }
            }
            Self::KeepConnection(state, conn) => {
                let x: Option<i64> = conn.get("counta").await.unwrap();
                if let Some(x) = x {
                    *state = x;
                }
            }
            Self::KeepConnectionFromPool(state, _, conn) => {
                let x: Option<i64> = conn.get("counta").await.unwrap();
                if let Some(x) = x {
                    *state = x;
                }
            }
        }
    }

    async fn do_external_request(&self) {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn simulate_request<'a>(mut db: DBStrategy<'a>) {
    db.do_a_read().await;
    db.do_a_read().await;
    db.do_external_request().await;
    db.do_a_write().await;
    db.do_a_write().await;
}

static ITERATIONS: usize = 1000;
static POOL_SIZE: u32 = 50;

#[tokio::main]
async fn main() {
    println!("\"Requests\": {}", ITERATIONS);
    println!("Pool size: {}", POOL_SIZE);
    println!("");

    {
        println!("Checking out on each operation");

        let redis_manager = RedisConnectionManager::new("redis://172.18.0.7:6379/9").unwrap();
        let redis_pool = bb8::Pool::builder()
            .max_size(POOL_SIZE)
            .build(redis_manager)
            .await
            .unwrap();

        let before = std::time::Instant::now();

        let mut handles = Vec::with_capacity(ITERATIONS);
        for _ in 1..ITERATIONS {
            let db = DBStrategy::CheckoutEveryTime(0, redis_pool.clone());
            handles.push(tokio::spawn(async move {
                simulate_request(db).await;
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        let after = std::time::Instant::now();
        let time = after.duration_since(before);

        println!("Took {:?}", time);
        println!("{} TPS", ITERATIONS as f64 / time.as_secs_f64());
    }

    {
        println!("\nNo pool");

        let before = std::time::Instant::now();

        let mut handles = Vec::with_capacity(ITERATIONS);
        for _ in 1..ITERATIONS {
            let redis = redis::Client::open("redis://172.18.0.7:6379/9").unwrap();
            handles.push(tokio::spawn(async move {
                let conn = redis.get_async_connection().await.unwrap();
                let db = DBStrategy::KeepConnection(0, conn);
                simulate_request(db).await;
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        let after = std::time::Instant::now();
        let time = after.duration_since(before);

        println!("Took {:?}", time);
        println!("{} TPS", ITERATIONS as f64 / time.as_secs_f64());
    }

    {
        println!("\nChecking out at beginning, keeping throughout request");

        let redis_manager = RedisConnectionManager::new("redis://172.18.0.7:6379/9").unwrap();
        let redis_pool = bb8::Pool::builder()
            .max_size(POOL_SIZE)
            .build(redis_manager)
            .await
            .unwrap();

        let before = std::time::Instant::now();

        let mut handles = Vec::with_capacity(ITERATIONS);
        for _ in 1..ITERATIONS {
            let pool = redis_pool.clone();
            handles.push(tokio::spawn(async move {
                let conn = pool.get().await.unwrap();
                let db = DBStrategy::KeepConnectionFromPool(0, &pool, conn);
                simulate_request(db).await;
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        let after = std::time::Instant::now();
        let time = after.duration_since(before);

        println!("Took {:?}", time);
        println!("{} TPS", ITERATIONS as f64 / time.as_secs_f64());
    }
}
