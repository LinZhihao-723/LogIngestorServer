## Generate and Upload (Ubuntu)

First, Start a MinIO server locally:

```shell
mkdir minio-data
docker run -d \
    --name minio \
    --restart unless-stopped \
    -p 9000:9000 \
    -p 9001:9001 \
    -v ./minio-data:/data \
    -e MINIO_ROOT_USER="minioadmin" \
    -e MINIO_ROOT_PASSWORD="minioadmin" \
    quay.io/minio/minio server /data --console-address ":9001"
```

Second, download minIO client and create a bucket called `integration-test`:

```shell
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
./mc alias set local http://localhost:9000 minioadmin minioadmin
./mc mb local/integration-test
```

Finally, run the script to generate and upload files to the bucket:
```

```shell
python3 generate_and_upload.py \
    --s3-endpoint http://localhost:9000 \
    --s3-key "minioadmin" \
    --s3-secret "minioadmin" \
    --s3-bucket "integration-test" \
    --num-files 1
```

You can check the uploaded files in the MinIO console at `http://localhost:9001`.

## Test the Server

First, start the server (replace `${MARIADB_PASSWORD}` with the actual password when starting the
CLP package):

```shell
cargo run --release -- \
    --db-url "mysql://clp-user:${MARIADB_PASSWORD}/clp-db" \
    --s3-endpoint http://localhost:9000
```

Then, create a scanning job (replace `${PREFIX}` with the actual prefix used in the upload step):

```shell
curl -v -u "minioadmin:minioadmin" \
"http://127.0.0.1:8080/create?region=test&bucket=integration-test&dataset=test&key_prefix=${PREFIX}"
```
