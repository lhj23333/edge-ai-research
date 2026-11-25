import hashlib
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .config import DATA, config_lock, prepare_data

SCHEMA_VERSION = 12


def now(*, timespec="seconds"):
    return datetime.now(timezone.utc).isoformat(timespec=timespec)


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
    with config_lock(), open_database() as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise RuntimeError("数据库版本高于当前程序，不能启动旧版程序")
        directory = Path(__file__).parent / "migrations"
        connection.create_function("sha256", 1, lambda value: hashlib.sha256(value.encode()).hexdigest(), deterministic=True)
        for target in range(version + 1, SCHEMA_VERSION + 1):
            script, = directory.glob(f"{target:03d}_*.sql")
            if target == 6:
                connection.execute("PRAGMA foreign_keys=OFF")
            connection.executescript(f"BEGIN IMMEDIATE;\n{script.read_text()}\nPRAGMA user_version={target};")
            if target == 11:
                from .station import merge_existing_product_aliases

                merge_existing_product_aliases(connection)
            if connection.execute("PRAGMA foreign_key_check").fetchone():
                connection.rollback()
                raise RuntimeError(f"数据库迁移 {target:03d} 的关联检查未通过")
            connection.commit()
            connection.execute("PRAGMA foreign_keys=ON")
    os.chmod(DATA / "researcher.sqlite3", 0o600)


@contextmanager
def open_database():
    connection = connect()
    try:
        yield connection
    finally:
        connection.close()


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
