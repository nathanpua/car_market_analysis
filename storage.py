"""Cloudflare R2 storage for persisting the SQLite DB across CI runs.

Downloads the DB before scraping and uploads it after.
Uses boto3 with the S3-compatible R2 API.
"""

import logging
import os
import time
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger(__name__)

DB_DIR = Path("output")
DB_PATH = DB_DIR / "scrapling_listings.db"
R2_DB_KEY = os.environ.get("R2_DB_KEY", "scrapling_listings.db")

_UPLOAD_RETRIES = 3
_RETRY_DELAY = 2  # seconds base, multiplied by attempt number

_REQUIRED_ENV_VARS = ("R2_ENDPOINT", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_BUCKET")


def _get_s3_client():
    """Create an S3 client configured for Cloudflare R2."""
    missing = [v for v in _REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    endpoint = os.environ["R2_ENDPOINT"]
    access_key = os.environ["R2_ACCESS_KEY"]
    secret_key = os.environ["R2_SECRET_KEY"]

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(
            region_name="auto",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def _get_bucket() -> str:
    return os.environ["R2_BUCKET"]


def download_db() -> Path:
    """Download DB from R2. Returns local path.

    If the object does not exist in R2 (first run), creates the output
    directory and returns the path anyway -- ListingDB.__init__ will
    create the DB file fresh.

    Retries up to 3 times on transient failures.
    """
    DB_DIR.mkdir(parents=True, exist_ok=True)

    client = _get_s3_client()
    bucket = _get_bucket()

    try:
        client.head_object(Bucket=bucket, Key=R2_DB_KEY)
    except client.exceptions.ClientError:
        logger.info("No existing DB in R2 (first run), will create fresh")
        return DB_PATH

    for attempt in range(1, _UPLOAD_RETRIES + 1):
        try:
            logger.info(
                "Downloading DB from R2: %s/%s (attempt %d)",
                bucket, R2_DB_KEY, attempt,
            )
            client.download_file(bucket, R2_DB_KEY, str(DB_PATH))
            size_mb = DB_PATH.stat().st_size / (1024 * 1024)
            logger.info("Downloaded DB (%.1f MB)", size_mb)
            return DB_PATH
        except Exception as e:
            logger.warning("Download failed (attempt %d): %s", attempt, e)
            if attempt < _UPLOAD_RETRIES:
                time.sleep(_RETRY_DELAY * attempt)

    raise RuntimeError(f"Download failed after {_UPLOAD_RETRIES} attempts")


def upload_db() -> None:
    """Upload DB to R2. Overwrites previous version.

    Retries up to 3 times on failure.
    """
    if not DB_PATH.exists():
        logger.warning("DB file not found at %s, skipping upload", DB_PATH)
        return

    client = _get_s3_client()
    bucket = _get_bucket()
    size_mb = DB_PATH.stat().st_size / (1024 * 1024)

    for attempt in range(1, _UPLOAD_RETRIES + 1):
        try:
            logger.info(
                "Uploading DB to R2: %s/%s (%.1f MB, attempt %d)",
                bucket, R2_DB_KEY, size_mb, attempt,
            )
            client.upload_file(str(DB_PATH), bucket, R2_DB_KEY)
            logger.info("Upload successful")
            return
        except Exception as e:
            logger.warning("Upload failed (attempt %d): %s", attempt, e)
            if attempt < _UPLOAD_RETRIES:
                time.sleep(_RETRY_DELAY * attempt)

    logger.error("Upload failed after %d attempts", _UPLOAD_RETRIES)
    raise RuntimeError(f"Upload failed after {_UPLOAD_RETRIES} attempts")


def delete_db() -> None:
    """Delete the DB from R2."""
    client = _get_s3_client()
    bucket = _get_bucket()
    client.delete_object(Bucket=bucket, Key=R2_DB_KEY)
    logger.info("Deleted DB from R2: %s/%s", bucket, R2_DB_KEY)


def get_db_info() -> dict:
    """Return metadata about the DB in R2: file size, last modified."""
    client = _get_s3_client()
    bucket = _get_bucket()

    try:
        resp = client.head_object(Bucket=bucket, Key=R2_DB_KEY)
        return {
            "size_bytes": resp["ContentLength"],
            "last_modified": resp["LastModified"].isoformat(),
            "exists": True,
        }
    except client.exceptions.ClientError:
        return {"exists": False}
