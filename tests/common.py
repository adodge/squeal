import os

import petname
import pymysql

from squeal import MySQLBackend

SQUEAL_TEST_HOSTNAME = os.environ.get("SQUEAL_TEST_HOSTNAME", "localhost")
SQUEAL_TEST_PORT = os.environ.get("SQUEAL_TEST_PORT", "3306")
SQUEAL_TEST_USERNAME = os.environ.get("SQUEAL_TEST_USERNAME", "root")
SQUEAL_TEST_PASSWORD = os.environ.get("SQUEAL_TEST_PASSWORD", "password")
SQUEAL_TEST_DATABASE = os.environ.get("SQUEAL_TEST_DATABASE", "test")


def get_connection():
    conn = pymysql.connect(
        host=SQUEAL_TEST_HOSTNAME,
        port=int(SQUEAL_TEST_PORT),
        user=SQUEAL_TEST_USERNAME,
        password=SQUEAL_TEST_PASSWORD,
    )
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SQUEAL_TEST_DATABASE}")
    conn.select_db(SQUEAL_TEST_DATABASE)
    conn.ping(reconnect=False)
    return conn


def temporary_name():
    return petname.Generate(3, separator="")


class TemporaryMySQLBackend(MySQLBackend):
    def __init__(self, **kwargs):
        if "connection" not in kwargs:
            kwargs["connection"] = get_connection()
        if "prefix" not in kwargs:
            kwargs["prefix"] = temporary_name()
        super().__init__(**kwargs)

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()
