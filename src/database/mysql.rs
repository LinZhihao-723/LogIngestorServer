use anyhow::Result;
use sqlx::mysql::MySqlPool;
use tokio::sync::OnceCell;

static POOL: OnceCell<MySqlPool> = OnceCell::const_new();

pub async fn init(database_url: &str) -> Result<()> {
    let pool = MySqlPool::connect(database_url).await?;
    POOL.set(pool)
        .map_err(|_| anyhow::anyhow!("Failed to set database pool"))?;
    Ok(())
}

pub async fn deinit() {
    if let Some(pool) = POOL.get() {
        pool.close().await;
    }
}

pub fn get_pool() -> MySqlPool {
    POOL.get()
        .expect("Database pool is not initialized.")
        .clone()
}
