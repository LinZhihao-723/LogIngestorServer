use anyhow::Result;

use super::config::JobConfig;

pub async fn submit_compression_job(job_config: JobConfig) -> Result<u64> {
    let pool = crate::database::mysql::get_pool();
    let res = sqlx::query(r#"INSERT INTO compression_jobs (`clp_config`) VALUES (?)"#)
        .bind(job_config.to_msgpack_brotli()?)
        .execute(&pool)
        .await?;

    Ok(res.last_insert_id())
}
