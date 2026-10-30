import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .config import DATA, config_lock, prepare_data

SCHEMA_VERSION = 3


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect():
    prepare_data()
    connection = sqlite3.connect(DATA / "researcher.sqlite3", timeout=20)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=20000")
    return connection


@contextmanager
def transaction():
    connection = connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def initialize():
    with config_lock(), transaction() as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise RuntimeError("数据库版本高于当前程序，不能启动旧版程序")
        if version < 1:
            script = (Path(__file__).parent / "migrations/001_initial.sql").read_text()
            connection.executescript("BEGIN IMMEDIATE;\n" + script + "\nPRAGMA user_version=1;\nCOMMIT;")
        if version < 2:
            script = (Path(__file__).parent / "migrations/002_station.sql").read_text()
            connection.executescript("BEGIN IMMEDIATE;\n" + script + "\nPRAGMA user_version=2;\nCOMMIT;")
        if version < 3:
            script = (Path(__file__).parent / "migrations/003_event_merge.sql").read_text()
            connection.executescript("BEGIN IMMEDIATE;\n" + script + "\nPRAGMA user_version=3;\nCOMMIT;")
    os.chmod(DATA / "researcher.sqlite3", 0o600)


def query(sql, parameters=()):
    connection = connect()
    try:
        return [dict(row) for row in connection.execute(sql, parameters).fetchall()]
    finally:
        connection.close()


def one(sql, parameters=()):
    rows = query(sql, parameters)
    return rows[0] if rows else None


def execute(sql, parameters=()):
    with transaction() as connection:
        cursor = connection.execute(sql, parameters)
        return cursor.lastrowid
