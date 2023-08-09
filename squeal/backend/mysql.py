import random
from typing import List, Tuple

from squeal import Message, QueueEmpty
from ..squeal import Backend, PAYLOAD_MAX_SIZE

SQL_CREATE = """
CREATE TABLE IF NOT EXISTS {name} (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    topic INT UNSIGNED NOT NULL,
    owner_id INT UNSIGNED NULL,
    acquire_time DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    payload VARBINARY({size}),
    PRIMARY KEY (id)
)
"""
SQL_DROP = "DROP TABLE IF EXISTS {name}"
SQL_INSERT = "INSERT INTO {name} (payload, topic) VALUES (%s, %s)"
SQL_UPDATE = """
UPDATE {name} SET owner_id=NULL
WHERE owner_id IS NOT NULL AND topic=%s
    AND TIMESTAMPDIFF(SECOND, acquire_time, NOW()) > %s
"""
SQL_UPDATE_2 = "UPDATE {name} SET owner_id=%s WHERE id=%s"
SQL_UPDATE_3 = "UPDATE {name} SET owner_id=NULL WHERE owner_id=%s AND id=%s"
SQL_SELECT = """
SELECT id, owner_id, payload FROM {name}
    WHERE owner_id IS NULL AND topic=%s
    ORDER BY id
    LIMIT 1 FOR UPDATE SKIP LOCKED;
"""
SQL_DELETE = "DELETE FROM {name} WHERE id=%s"
SQL_COUNT = "SELECT count(1) FROM {name} WHERE topic=%s AND owner_id IS NULL"
SQL_COUNT_2 = "SELECT topic, count(*) FROM {name} WHERE owner_id IS NULL GROUP BY topic"


class MySQLBackend(Backend):
    def __init__(self, connection, prefix: str, acquire_timeout: int, *args, **kwargs):
        """
        :param connection: https://peps.python.org/pep-0249/#connection-objects
        """
        super().__init__(*args, **kwargs)
        self.connection = connection
        self.prefix = prefix
        self.queue_table = f"{self.prefix}_queue"
        self.acquire_timeout = acquire_timeout
        self.owner_id = random.randint(0, 2**32 - 1)

    def create(self) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_CREATE.format(name=self.queue_table, size=PAYLOAD_MAX_SIZE))
            self.connection.commit()

    def destroy(self) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_DROP.format(name=self.queue_table))
            self.connection.commit()

    def put(self, item: bytes, topic: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_INSERT.format(name=self.queue_table), args=(item, topic))
            self.connection.commit()

    def release_stalled_tasks(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                SQL_UPDATE.format(name=self.queue_table),
                args=(topic, self.acquire_timeout),
            )
            rows = cur.rowcount
            self.connection.commit()
            return rows

    def get(self, topic: int) -> "Message":
        with self.connection.cursor() as cur:
            self.connection.begin()

            cur.execute(SQL_SELECT.format(name=self.queue_table), args=(topic,))

            row = cur.fetchone()
            if row is None:
                self.connection.rollback()
                raise QueueEmpty()

            cur.execute(
                SQL_UPDATE_2.format(name=self.queue_table), args=(self.owner_id, row[0])
            )

            self.connection.commit()

        return Message(row[2], row[0], self)

    def ack(self, task_id: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_DELETE.format(name=self.queue_table), args=(task_id,))
            self.connection.commit()

    def nack(self, task_id: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                SQL_UPDATE_3.format(name=self.queue_table),
                args=(self.owner_id, task_id),
            )
            self.connection.commit()

    def size(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_COUNT.format(name=self.queue_table), args=(topic,))
            result = cur.fetchone()
            self.connection.commit()
            return result[0]

    def topics(self) -> List[Tuple[int, int]]:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_COUNT_2.format(name=self.queue_table))
            rows = cur.fetchall()
            self.connection.commit()
        return rows


__all__ = ["MySQLBackend"]
