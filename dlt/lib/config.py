import os
import dlt
import logging


## ========================================================
## DLT configuration
## ========================================================

def is_production() -> bool:
    return os.environ.get("DLT_ENV") == "production"

BATCH_SIZE = 1_000_000

# Pipeline iteration timeout (in seconds)
PIPELINE_ITERATION_TIMEOUT = int(os.environ.get("DLT_PIPELINE_ITERATION_TIMEOUT", "3600"))

def configure_env() -> None:
    """Configure the environment for the DLT pipelines."""
    configure_dlt()
    configure_rustfs_filesystem_destination()
    configure_beefy_db_source()
    configure_clickhouse_destination()

def configure_dlt() -> None:
    """Configure dlt from environment variables."""
    # Set runtime configuration via environment variables
    if "RUNTIME__LOG_LEVEL" in os.environ:
        dlt.config["runtime.log_level"] = os.environ["RUNTIME__LOG_LEVEL"]

    dlt.config["load.truncate_staging_dataset"] = True
    dlt.config["truncate_staging_dataset"] = True
    os.environ['LOAD__TRUNCATE_STAGING_DATASET'] = 'true'

    os.environ['EXTRACT__WORKERS'] = "3"
    os.environ['EXTRACT__DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
    os.environ['EXTRACT__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(BATCH_SIZE)
    os.environ['EXTRACT__DATA_WRITER__FILE_MAX_ITEMS'] = str(BATCH_SIZE)
    os.environ['NORMALIZE__WORKERS'] = '3'
    os.environ['NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
    os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(BATCH_SIZE)
    os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = str(BATCH_SIZE)
    os.environ["LOAD__WORKERS"] = "3"
    os.environ['LOAD__DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
    os.environ['LOAD__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(BATCH_SIZE)
    os.environ['LOAD__DATA_WRITER__FILE_MAX_ITEMS'] = str(BATCH_SIZE)

    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('dlt')
    logger.setLevel(logging.INFO)
    logger = logging.getLogger('dlt.source.sql')
    logger.setLevel(logging.INFO)
    logger = logging.getLogger('dlt.destination.clickhouse')
    logger.setLevel(logging.WARNING)
    logger = logging.getLogger('sqlalchemy.engine')
    logger.setLevel(logging.WARNING)



## ========================================================
## RustFS (S3-compatible) filesystem destination configuration
## ========================================================

def configure_rustfs_filesystem_destination() -> None:
    """
    [destination.filesystem]
    bucket_url = "s3://[your_bucket_name]"

    [destination.filesystem.credentials]
    aws_access_key_id = "..."
    aws_secret_access_key = "..."
    endpoint_url = "..."
    """

    # In prod, dlt and ClickHouse use RustFS (S3-compatible). In dev, dlt uses local file storage.

    if is_production():
        rustfs_bucket_name = os.environ.get("RUSTFS_DLT_STAGING_BUCKET")
        rustfs_access_key_id = os.environ.get("RUSTFS_ACCESS_KEY")
        rustfs_secret_access_key = os.environ.get("RUSTFS_SECRET_KEY")
        rustfs_endpoint = os.environ.get("RUSTFS_ENDPOINT")

        if not all([rustfs_bucket_name, rustfs_access_key_id, rustfs_secret_access_key, rustfs_endpoint]):
            raise ValueError("All RustFS configuration must be set")

        dlt.config["destination.filesystem"] = {
            "bucket_url": f"s3://{rustfs_bucket_name}",
        }

        dlt.secrets["destination.filesystem.credentials"] = {
            "aws_access_key_id": rustfs_access_key_id,
            "aws_secret_access_key": rustfs_secret_access_key,
            "endpoint_url": rustfs_endpoint,
        }
    else:
        storage_dir = os.environ.get("STORAGE_DIR")
        if not storage_dir:
            raise ValueError("STORAGE_DIR environment variable must be set for non-production environments")
        dst_dir = storage_dir + "/dlt"
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
            
        dlt.config["destination.filesystem"] = {
            "bucket_url": f"file://{dst_dir}",
        }


## ========================================================
## Beefy DB source configuration
## ========================================================

def configure_beefy_db_source() -> None:
    """Configure dlt from environment variables."""
    # Set runtime configuration via environment variables
    if "RUNTIME__LOG_LEVEL" in os.environ:
        dlt.config["runtime.log_level"] = os.environ["RUNTIME__LOG_LEVEL"]

    # Configure Beefy DB source credentials from environment variables
    beefy_db_host = os.environ.get("BEEFY_DB_HOST")
    beefy_db_port = os.environ.get("BEEFY_DB_PORT")
    beefy_db_name = os.environ.get("BEEFY_DB_NAME")
    beefy_db_user = os.environ.get("BEEFY_DB_USER")
    beefy_db_password = os.environ.get("BEEFY_DB_PASSWORD")
    beefy_db_sslmode = os.environ.get("BEEFY_DB_SSLMODE") or "require"

    if beefy_db_host:
        dlt.secrets["source.beefy_db.credentials"] = f"postgresql://{beefy_db_user}:{beefy_db_password}@{beefy_db_host}:{beefy_db_port}/{beefy_db_name}?sslmode={beefy_db_sslmode}"


def get_beefy_db_url() -> str:
    return dlt.secrets["source.beefy_db.credentials"]


## ========================================================
## ClickHouse destination configuration
## ========================================================

def get_clickhouse_credentials() -> str:

    clickhouse_host = os.environ.get("DLT_CLICKHOUSE_HOST")
    clickhouse_user = os.environ.get("DLT_CLICKHOUSE_USER")
    clickhouse_password = os.environ.get("DLT_CLICKHOUSE_PASSWORD")
    clickhouse_database = os.environ.get("DLT_CLICKHOUSE_DB")
    clickhouse_port = int(os.environ.get("DLT_CLICKHOUSE_PORT", "9000"))
    clickhouse_http_port = int(os.environ.get("DLT_CLICKHOUSE_HTTP_PORT", "8123"))
    clickhouse_secure = int(os.environ.get("DLT_CLICKHOUSE_SECURE", "0"))

    if not all([clickhouse_host, clickhouse_user, clickhouse_password, clickhouse_database]):
        raise ValueError("All ClickHouse credentials must be set")

    return {
        "host": clickhouse_host,
        "port": clickhouse_port,
        "http_port": clickhouse_http_port,
        "user": clickhouse_user,
        "password": clickhouse_password,
        "database": clickhouse_database,
        "secure": clickhouse_secure,
    }


def configure_clickhouse_destination() -> None:
    """Configure dlt from environment variables."""
    # Set runtime configuration via environment variables
    if "RUNTIME__LOG_LEVEL" in os.environ:
        dlt.config["runtime.log_level"] = os.environ["RUNTIME__LOG_LEVEL"]

    credentials = get_clickhouse_credentials()
    host = credentials["host"]
    port = credentials["port"]
    http_port = credentials["http_port"]
    user = credentials["user"]
    password = credentials["password"]
    database = credentials["database"]
    secure = credentials["secure"]
    dlt.secrets["destination.clickhouse.credentials"] = f"clickhouse://{user}:{password}@{host}:{port}/{database}?secure={secure}"

    os.environ['DESTINATION__CLICKHOUSE__CREDENTIALS__HOST'] = os.environ.get("DLT_CLICKHOUSE_HOST", "localhost")
    os.environ['DESTINATION__CLICKHOUSE__CREDENTIALS__PORT'] = os.environ.get("DLT_CLICKHOUSE_PORT", "9000")
    os.environ['DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT'] = os.environ.get("DLT_CLICKHOUSE_HTTP_PORT", "8123")
    os.environ['DESTINATION__CLICKHOUSE__CREDENTIALS__USER'] = os.environ.get("DLT_CLICKHOUSE_USER", "dlt")
    os.environ['DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD'] = os.environ.get("DLT_CLICKHOUSE_PASSWORD", "changeme")
    os.environ['DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE'] = os.environ.get("DLT_CLICKHOUSE_DB", "dlt")
    os.environ['DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE'] = os.environ.get("DLT_CLICKHOUSE_SECURE", "0")
    
