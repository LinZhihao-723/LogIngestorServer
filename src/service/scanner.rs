use super::manager::ScannerServiceManager;
use crate::scanner::JobParams;
use actix_web::{HttpResponse, Responder, get, web};
use actix_web_httpauth::extractors::basic::BasicAuth;
use serde::Deserialize;

#[get("/create")]
pub async fn create_scanner_job(
    service_mgr: web::Data<ScannerServiceManager>,
    auth: BasicAuth,
    query: web::Query<JobParams>,
) -> impl Responder {
    match service_mgr.create_job(auth, query.into_inner()).await {
        Ok(id) => HttpResponse::Ok().body(id.to_string()),
        Err(e) => {
            let error_msg = format!("Failed to create job: {}", e);
            log::error!("{}", error_msg.as_str());
            HttpResponse::InternalServerError().body(error_msg)
        }
    }
}

#[derive(Deserialize)]
struct JobIdQuery {
    job_id: String,
}

#[get("/delete")]
pub async fn delete_scanner_job(
    service_mgr: web::Data<ScannerServiceManager>,
    query: web::Query<JobIdQuery>,
) -> impl Responder {
    match service_mgr.delete_job(query.job_id.as_str()).await {
        Ok(_) => HttpResponse::Ok().body(format!("Deleted job: {}", query.job_id)),
        Err(e) => HttpResponse::BadRequest().body(format!("Error: {}", e)),
    }
}
