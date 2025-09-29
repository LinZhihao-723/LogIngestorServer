use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client,
    config::{Builder, Credentials, Region},
};
use secrecy::{ExposeSecret, SecretString};

pub async fn create_s3_client(
    endpoint: &str,
    region_id: &str,
    access_key_id: &str,
    secret_access_key: &SecretString,
) -> Client {
    let credential = Credentials::new(
        access_key_id,
        secret_access_key.expose_secret(),
        None,
        None,
        "User",
    );
    let region = Region::new(region_id.to_owned());
    let base_config = aws_config::defaults(BehaviorVersion::v2025_08_07())
        .region(region.clone())
        .endpoint_url(endpoint)
        .load()
        .await;
    let config = Builder::from(&base_config)
        .credentials_provider(credential)
        .force_path_style(true)
        .region(region)
        .build();
    Client::from_conf(config)
}
