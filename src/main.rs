mod buffering;
mod compression;
mod database;
mod scanner;
mod service;
mod sqs_listener;
mod utils;

use actix_web::{App, HttpServer, web};
use clap::Parser;
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming};
use service::{
    ScannerServiceManager,
    service_method::{create_scanner_job, create_sqs_listener_job, delete_job},
};

#[derive(Parser)]
struct Args {
    #[clap(long)]
    db_url: String,

    #[clap(long, help = "Optional S3 endpoint for connecting to S3-compatible storage.")]
    s3_endpoint: Option<String>,

    #[clap(long, default_value = "127.0.0.1", help = "Host to bind the server to.")]
    host: String,

    #[clap(long, default_value_t = 8080, help = "Port to bind the server to.")]
    port: u16,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // TODO: Handle logger initialization errors
    // TODO: Make the logger async friendly: consider using https://docs.rs/tracing/latest/tracing/
    Logger::try_with_env_or_str("info")
        .unwrap()
        .log_to_file(
            FileSpec::default()
                .directory(".logs")
                .basename("server")
                .suffix("log"),
        )
        .duplicate_to_stdout(Duplicate::All)
        .rotate(
            Criterion::Size(10_000_000),
            Naming::Numbers,
            Cleanup::KeepLogFiles(7),
        )
        .use_utc() // optional: timestamps in UTC; remove for local time
        .format(flexi_logger::detailed_format)
        .start()
        .expect("failed to initialize logging");

    log::info!("Starting server.");

    let args = Args::parse();

    // Initialize database connection
    match database::mysql::init(&args.db_url).await {
        Ok(()) => log::info!("Database initialized successfully."),
        Err(e) => {
            log::error!("Failed to initialize database: {}. Url: {}", e, args.db_url);
            return Err(std::io::Error::other("Database init failed."));
        }
    }

    // Initialize service manager
    let scanner_service_manager = web::Data::new(ScannerServiceManager::new(
        100,                                // listener channel size
        std::time::Duration::from_secs(60), // listener channel timeout
        args.s3_endpoint.clone(),           // optional S3 endpoint
        10 * 1024 * 1024,                   // buffer size (bytes)
    ));

    HttpServer::new(move || {
        App::new()
            .app_data(scanner_service_manager.clone())
            .service(create_scanner_job)
            .service(create_sqs_listener_job)
            .service(delete_job)
    })
    .bind((args.host, args.port))?
    .run()
    .await?;

    database::mysql::deinit().await;
    Ok(())
}
