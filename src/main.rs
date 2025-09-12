use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use actix_web_httpauth::extractors::basic::BasicAuth;
use secrecy::{ExposeSecret, SecretString};

#[get("/s3-op")]
async fn s3_op(auth: BasicAuth) -> impl Responder {
    // Extract credentials (do NOT log these)
    let access_key_id = auth.user_id().to_owned();
    let secret_access_key = SecretString::new(
        auth.password().unwrap_or_default().to_owned()
    );

    let access_key = secret_access_key.expose_secret();
    println!("Access Key ID: {}, Secret Access Key: {}\n", access_key_id, access_key);

    HttpResponse::Ok().finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // IMPORTANT: serve behind HTTPS (TLS termination at a proxy),
    // or configure Rustls on HttpServer (not shown here to keep it short).
    HttpServer::new(|| App::new().service(s3_op))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
