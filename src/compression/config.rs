use anyhow::Result;
use brotli::CompressorWriter;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobConfig {
    pub input: Input,
    pub output: Output,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Input {
    pub aws_authentication: AwsAuthentication,
    pub bucket: String,
    pub dataset: String,
    pub key_prefix: String,
    pub region_code: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum AwsAuthentication {
    #[serde(rename = "credentials")]
    Credentials { credentials: AwsCredentials },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct Output {
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

    pub fn serialize_to_msgpack_brotli_str(&self) -> Result<String> {
        let compressed_data = self.to_msgpack_brotli()?;
        Ok(hex::encode(compressed_data))
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

        let serialized_result = config.serialize_to_msgpack_brotli_str();
        assert!(serialized_result.is_ok());
        let serialized = serialized_result.unwrap();
        let expected = "1b610100e4f8fbb900194983555814ddcfbe7b2b2cb24e1bd80e6fb10ea7fc7d74\
             1b6df4d12cecc74cb340b230e726f476672375d125c174059c2deefc1081673413e\
             9a21f305dc660e020c6836e1cd2790b67989e78fd04531e832e9db2f97cb1d9847a\
             b10fd5c28de005600fb76631df28b600ba5c0b4c34655a8fc5b69b444d479936e73\
             2fbd2cc1ff7eb8049dc72554cdebdcfc0e5454d1212a2e525264807a20648951dc3\
             b886399e595b341637d1b5a523836a10a38f0f453c08f706a84fe47f2c140a7d174\
             f318faccb88c023fca9b2900239ed1785797b22";
        assert_eq!(expected, serialized);
    }
}
