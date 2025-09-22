#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ListenerKey {
    dataset: Option<String>,
    access_key_id: String,
    secret_access_key: String,
}

impl ListenerKey {
    pub fn new(dataset: Option<String>, access_key_id: String, secret_access_key: String) -> Self {
        Self {
            dataset,
            access_key_id,
            secret_access_key,
        }
    }

    pub fn get_dataset(&self) -> Option<&str> {
        self.dataset.as_deref()
    }

    pub fn get_access_key_id(&self) -> &str {
        &self.access_key_id
    }

    pub fn get_secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
}
