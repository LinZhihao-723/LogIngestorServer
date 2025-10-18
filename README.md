# Log Ingestor for CLP

This is a prototype log ingestor server for ingesting logs from S3-compatible storage services into
CLP.

## Prerequisites

- CLP version higher than [this][clp-version-required] commit.
- Rust toolchain installed. You can follow the instructions at [rustup.rs] to install Rust.

## Ingest Logs to S3-Compatible Storage

### Step 1: Start CLP Package

Start the CLP package following the instructions in CLP's doc.

In the started package, check `etc/credentials.yml`'s `database` config and `etc/clp-config.yml` to
generate a database URL for connection. For example:

```yaml
# etc/credentials.yml
database:
  password: JSb8t39AGkA
  user: clp-user

# etc/clp-config.yml
database:
  type: "mariadb"
  host: "localhost"
  port: 3306
  name: "clp-db"
```

With such a config, the database URL will be:

```
mysql://clp-user:JSb8t39AGkA@localhost:3306/clp-db
```

### Step 2: Start the Ingestor Server

Start the ingestor server with the URL generated in Step 1:

```shell
cargo run --release -- --db-url "$CLP_DB_URL"
```

If you want to host the server on a different address or port, you can specify them with `--host`
and `--port` options.

### Step 3: Create Log Ingestion Jobs

The current server supports two types of ingestion jobs:

* S3 Scanner Job: Periodically scans an S3 bucket and ingest newly ingested objects that match a
  given key prefix.
* SQS Listener Job: Listens to an SQS queue for messages that contains object creation events and
  ingest objects that match a given key prefix.

#### S3 Scanner Job

Use `curl` to create the following request to create an S3 scanner job:

```shell
curl -v -u "AWS_ACCESS_KEY:AWS_SECRETE_KEY" "http://127.0.0.1:8080/scanner/create?region={$REGION}bucket={$BUCKET}&key_prefix={$KEY_PREFIX}&dataset={$DATASET}"
```

Replace `AWS_ACCESS_KEY` and `AWS_SECRETE_KEY` with your S3 credentials, and replace
`{$REGION}`, `{$BUCKET}`, `{$KEY_PREFIX}`, and `{$DATASET}` with the S3 region, bucket name, key
prefix, and dataset name respectively.

#### SQS Listener Job

Use `curl` to create the following request to create an SQS listener job:

```shell
curl -v -u "AWS_ACCESS_KEY:AWS_SECRETE_KEY" "http://127.0.0.1:8080/sqs_listener/create?region={$REGION}bucket={$BUCKET}&key_prefix={$KEY_PREFIX}&dataset={$DATASET}&sqs_url={$SQS_URL}"
```

Replace `AWS_ACCESS_KEY` and `AWS_SECRETE_KEY` with your S3 credentials, and replace
`{$REGION}`, `{$BUCKET}`, `{$KEY_PREFIX}`, `{$DATASET}` and `{$SQS_URL}` with the S3 region, bucket
name, key prefix, dataset name, and SQS URL respectively.

NOTE:

* `SQS_URL` must be an encoded URL. You can use [this] tool to encode the URL.
* The given credential must have permission to access the SQS queue, including `sqs:ReceiveMessage`,
  `sqs:DeleteMessage`, and `sqs:GetQueueAttributes`.

### Step 4 (optional): Cancel Jobs

The above methods will return a job ID upon successful creation. You can use the returned job ID to
cancel the job:

```shell
curl "http://127.0.0.1:8080/delete?job_id={$JOB_ID}"
```

[clp-version-required]: https://github.com/y-scope/clp/tree/e6b4a203aaa64415e28287963f99ea35c7c466ee
[rustup.rs]: https://rustup.rs/
[url-encode-tool]: https://meyerweb.com/eric/tools/dencoder/
