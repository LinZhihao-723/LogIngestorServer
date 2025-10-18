use actix_web::{HttpResponse, Responder, get, web};
use actix_web_httpauth::extractors::basic::BasicAuth;
use serde::Deserialize;

use super::manager::ScannerServiceManager;

#[get("/scanner/create")]
pub async fn create_scanner_job(
    service_mgr: web::Data<ScannerServiceManager>,
    auth: BasicAuth,
    query: web::Query<crate::scanner::JobParams>,
) -> impl Responder {
    let job_id = service_mgr
        .create_scanner_job(&auth, query.into_inner())
        .await;
    HttpResponse::Ok().body(job_id.to_string())
}

#[get("/sqs_listener/create")]
pub async fn create_sqs_listener_job(
    service_mgr: web::Data<ScannerServiceManager>,
    auth: BasicAuth,
    query: web::Query<crate::sqs_listener::JobParams>,
) -> impl Responder {
    let job_id = service_mgr
        .create_sqs_listener_job(&auth, query.into_inner())
        .await;
    HttpResponse::Ok().body(job_id.to_string())
}

#[derive(Deserialize)]
struct JobIdQuery {
    job_id: String,
}

#[get("/delete")]
pub async fn delete_job(
    service_mgr: web::Data<ScannerServiceManager>,
    query: web::Query<JobIdQuery>,
) -> impl Responder {
    match service_mgr.delete_job(query.job_id.as_str()).await {
        Ok(()) => HttpResponse::Ok().body(format!("Deleted job: {}", query.job_id)),
        Err(e) => HttpResponse::BadRequest().body(format!("Error: {e}")),
    }
}
