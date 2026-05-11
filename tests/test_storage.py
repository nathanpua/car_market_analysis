"""Tests for storage.py using moto S3 mock."""

import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest

from storage import DB_PATH, download_db, upload_db, get_db_info

BUCKET = "test-bucket"


@pytest.fixture(autouse=True)
def _mock_s3(monkeypatch, tmp_path):
    """Replace storage module's DB_PATH with tmp_path and mock S3."""
    import moto as _moto

    # Patch env vars and DB paths before importing storage logic
    monkeypatch.setenv("R2_BUCKET", BUCKET)
    monkeypatch.setattr("storage.DB_PATH", tmp_path / "scrapling_listings.db")
    monkeypatch.setattr("storage.DB_DIR", tmp_path)

    with _moto.mock_aws():
        # Create mock bucket (no endpoint_url so moto intercepts via its mock)
        s3 = boto3.client(
            "s3",
            region_name="us-east-1",
        )
        s3.create_bucket(Bucket=BUCKET)

        # Patch storage._get_s3_client to return this mock client
        # and _get_bucket to return test bucket
        monkeypatch.setattr("storage._get_bucket", lambda: BUCKET)

        def _make_mock_client():
            return boto3.client("s3", region_name="us-east-1")

        monkeypatch.setattr("storage._get_s3_client", _make_mock_client)
        yield s3


class TestDownloadDb:
    def test_download_existing_db(self, _mock_s3, tmp_path):
        test_db = tmp_path / "scrapling_listings.db"
        conn = sqlite3.connect(str(test_db))
        conn.execute("CREATE TABLE listings (id INTEGER)")
        conn.execute("INSERT INTO listings VALUES (42)")
        conn.commit()
        conn.close()

        _mock_s3.upload_file(
            str(test_db), BUCKET, "scrapling_listings.db",
        )
        test_db.unlink()

        result = download_db()
        assert result == test_db
        assert test_db.exists()

        conn = sqlite3.connect(str(test_db))
        rows = conn.execute("SELECT id FROM listings").fetchall()
        conn.close()
        assert rows == [(42,)]

    def test_download_missing_db_first_run(self, _mock_s3, tmp_path):
        test_db = tmp_path / "scrapling_listings.db"
        assert not test_db.exists()

        result = download_db()
        assert result == test_db
        assert not test_db.exists()


class TestUploadDb:
    def test_upload_db(self, _mock_s3, tmp_path):
        test_db = tmp_path / "scrapling_listings.db"
        conn = sqlite3.connect(str(test_db))
        conn.execute("CREATE TABLE listings (id INTEGER)")
        conn.execute("INSERT INTO listings VALUES (99)")
        conn.commit()
        conn.close()

        upload_db()

        resp = _mock_s3.head_object(Bucket=BUCKET, Key="scrapling_listings.db")
        assert resp["ContentLength"] > 0

    def test_upload_missing_db_skips(self, _mock_s3, tmp_path, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            upload_db()

        with pytest.raises(Exception):
            _mock_s3.head_object(Bucket=BUCKET, Key="scrapling_listings.db")


class TestRoundTrip:
    def test_upload_then_download(self, _mock_s3, tmp_path):
        test_db = tmp_path / "scrapling_listings.db"

        conn = sqlite3.connect(str(test_db))
        conn.execute("CREATE TABLE listings (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO listings VALUES (1, 'Honda Civic')")
        conn.commit()
        conn.close()

        upload_db()
        test_db.unlink()
        assert not test_db.exists()

        download_db()
        assert test_db.exists()

        conn = sqlite3.connect(str(test_db))
        rows = conn.execute("SELECT name FROM listings WHERE id = 1").fetchall()
        conn.close()
        assert rows == [("Honda Civic",)]


class TestGetDbInfo:
    def test_info_existing_db(self, _mock_s3, tmp_path):
        test_db = tmp_path / "scrapling_listings.db"
        test_db.write_text("test content")

        upload_db()
        info = get_db_info()
        assert info["exists"] is True
        assert info["size_bytes"] > 0

    def test_info_missing_db(self, _mock_s3):
        info = get_db_info()
        assert info["exists"] is False
