use anyhow::Result;
use brotli::CompressorWriter;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobConfig {
    pub input: Input,
    pub output: Output,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Input {
    pub aws_authentication: AwsAuthentication,
    pub bucket: String,
    pub dataset: String,
    pub key_prefix: String,
    pub region_code: String,
    pub keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum AwsAuthentication {
    #[serde(rename = "credentials")]
    Credentials { credentials: AwsCredentials },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Output {
    pub compression_level: u8,
    pub target_archive_size: u64,
    pub target_dictionaries_size: u64,
    pub target_encoded_file_size: u64,
    pub target_segment_size: u64,
}

impl JobConfig {
    pub fn to_msgpack_brotli(&self) -> Result<Vec<u8>> {
        let msgpack_data = rmp_serde::to_vec_named(self)?;
        let mut encoder = CompressorWriter::new(Vec::new(), 4096, 5, 22);
        std::io::copy(&mut &msgpack_data[..], &mut encoder)?;
        let compressed_data = encoder.into_inner();
        Ok(compressed_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_config_serialization() {
        let config = JobConfig {
            input: Input {
                aws_authentication: AwsAuthentication::Credentials {
                    credentials: AwsCredentials {
                        access_key_id: "ACCESS_KEY_ID".into(),
                        secret_access_key: "SECRET_ACCESS_KEY".into(),
                    },
                },
                bucket: "yscope".into(),
                dataset: "default".into(),
                key_prefix: "sample-logs/cockroachdb.clp.zst".into(),
                keys: None,
                region_code: "us-east-2".into(),
            },
            output: Output {
                compression_level: 3,
                target_archive_size: 268_435_456,
                target_dictionaries_size: 33_554_432,
                target_encoded_file_size: 268_435_456,
                target_segment_size: 268_435_456,
            },
        };

        let serialized_result = config.to_msgpack_brotli();
        assert!(serialized_result.is_ok());
        let serialized = serialized_result.unwrap();
        let expected = "1b670100e4686e1b84422a1e9a49134b37f5fbc7a2a6e4da2024066fb20ea7fc1eed5\
                7f4280bfb9869164816e6dce48fce6ca42eba24375e0267f31bdf47e029cd9ae8a21f6ebc88c1b8bd18\
                0fba7188ce5b38fcf8c8eb2798f21074e998ce66f3f53a54f35d28e7ae042f007bb8d6f3d95abe01d0e\
                54aa0a22993d5506cbd4ed40c2893f5b9d4ae34f1c77c3b6012b55c15e377ef337079519384986879b1\
                11923d5105a4da2dc5a8865534b5e83aff5b1935ceaea26b0b8826562284efdf8a5890ef0d501fc9ff0\
                8c3309fc553d8a3d16544c021fa6069c8829cf6b3c2b44901";
        assert_eq!(expected, hex::encode(serialized));
    }
}
