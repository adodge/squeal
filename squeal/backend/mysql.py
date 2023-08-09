import random
from typing import List, Tuple

from squeal import Message, QueueEmpty
from squeal.squeal import Backend, PAYLOAD_MAX_SIZE


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
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.queue_table} (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            topic INT UNSIGNED NOT NULL,
            owner_id INT UNSIGNED NULL,
            acquire_time DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
            payload VARBINARY({PAYLOAD_MAX_SIZE}),
            PRIMARY KEY (id)
        )
        """
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(sql)
            self.connection.commit()

    def destroy(self) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(f"DROP TABLE IF EXISTS {self.queue_table}")
            self.connection.commit()

    def put(self, item: bytes, topic: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"INSERT INTO {self.queue_table} (payload, topic) VALUES (%s, %s)",
                args=(item, topic),
            )
            self.connection.commit()

    def release_stalled_tasks(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"""
                UPDATE {self.queue_table} SET owner_id=NULL
                WHERE owner_id IS NOT NULL AND topic=%s
                    AND TIMESTAMPDIFF(SECOND, acquire_time, NOW()) > %s
            """,
                args=(topic, self.acquire_timeout),
            )
            rows = cur.rowcount
            self.connection.commit()
            return rows

    def get(self, topic: int) -> "Message":
        with self.connection.cursor() as cur:
            self.connection.begin()

            cur.execute(
                f"""
                SELECT id, owner_id, payload FROM {self.queue_table}
                    WHERE owner_id IS NULL AND topic=%s
                    ORDER BY id
                    LIMIT 1 FOR UPDATE SKIP LOCKED;
            """,
                args=(topic,),
            )

            row = cur.fetchone()
            if row is None:
                self.connection.rollback()
                raise QueueEmpty()

            cur.execute(
                f"UPDATE {self.queue_table} SET owner_id={self.owner_id} WHERE id=%s",
                args=(row[0],),
            )

            self.connection.commit()

        return Message(row[2], row[0], self)

    def ack(self, task_id: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(f"DELETE FROM {self.queue_table} WHERE id=%s", args=(task_id,))
            self.connection.commit()

    def nack(self, task_id: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"UPDATE {self.queue_table} SET owner_id=NULL WHERE id=%s AND owner_id=%s",
                args=(task_id, self.owner_id),
            )
            self.connection.commit()

    def size(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"SELECT count(*) FROM {self.queue_table} WHERE topic=%s AND owner_id IS NULL",
                args=(topic,),
            )
            result = cur.fetchone()
            self.connection.commit()
            return result[0]

    def topics(self) -> List[Tuple[int, int]]:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"SELECT topic, count(*) FROM {self.queue_table} "
                f"WHERE owner_id IS NULL GROUP BY topic"
            )
            rows = cur.fetchall()
            self.connection.commit()
        return rows


__all__ = ["MySQLBackend"]
