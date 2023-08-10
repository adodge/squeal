import random
from typing import List, Tuple
from .base import Backend, Message, QueueEmpty

PAYLOAD_MAX_SIZE = 1023

# Create a table to store queue items
# format args:
#   name -> table name
#   size -> max message size (bytes)
SQL_CREATE = """
CREATE TABLE IF NOT EXISTS {name} (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    topic INT UNSIGNED NOT NULL,
    priority INT UNSIGNED NOT NULL,
    owner_id INT UNSIGNED NULL,
    delivery_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    visibility_timeout INT UNSIGNED NOT NULL,
    failure_base_delay INT UNSIGNED NOT NULL,
    failure_count INT UNSIGNED DEFAULT 0,
    acquire_time DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    payload VARBINARY({size}),
    PRIMARY KEY (id)
)
"""

# Destroy the table
# format args:
#   name -> table name
SQL_DROP = "DROP TABLE IF EXISTS {name}"

# Insert a row into the queue
# format args:
#   name -> table name
# sql substitution args:
# * payload
# * topic
# * priority
# * delay (seconds)
# * failure_base_delay (seconds)
# * visibility timeout (seconds)
SQL_INSERT = (
    "INSERT INTO {name} (payload, topic, priority, delivery_time, failure_base_delay, visibility_timeout)"
    "VALUES (%s, %s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIME), %s, %s)"
)

# Release stalled messages
# Insert a row into the queue
# format args:
#   name -> table name
# sql substitution args:
# * topic
SQL_UPDATE = """
UPDATE {name} SET owner_id=NULL
WHERE owner_id IS NOT NULL AND topic=%s
    AND TIMESTAMPDIFF(SECOND, acquire_time, NOW()) > visibility_timeout
"""

# Acquire a task
# format args:
#   name -> table name
SQL_UPDATE_2 = "UPDATE {name} SET owner_id=%s WHERE id=%s"

# Release a task
# format args:
#   name -> table name
# sql substitution args:
# * owner id
# * message id
SQL_UPDATE_3 = """
UPDATE {name}
   SET owner_id=NULL,
       delivery_time=TIMESTAMPADD(SECOND, failure_base_delay * POW(2, failure_count), CURRENT_TIME),
       failure_count = failure_count + 1
   WHERE owner_id=%s AND id=%s
"""

# Find a task to acquire
SQL_SELECT = """
SELECT id, owner_id, payload FROM {name}
    WHERE owner_id IS NULL AND topic=%s AND TIMESTAMPDIFF(SECOND, delivery_time, NOW()) >= 0
    ORDER BY priority DESC, id ASC
    LIMIT 1 FOR UPDATE SKIP LOCKED;
"""

SQL_BATCH_SELECT = """
SELECT id, owner_id, payload FROM {name}
    WHERE owner_id IS NULL AND topic=%s AND TIMESTAMPDIFF(SECOND, delivery_time, NOW()) >= 0
    ORDER BY priority DESC, id ASC
    LIMIT %s FOR UPDATE SKIP LOCKED;
"""
SQL_BATCH_UPDATE = "UPDATE {name} SET owner_id=%s WHERE id IN %s"

# Finish a task
SQL_DELETE = "DELETE FROM {name} WHERE id=%s"

# Count tasks in topic
SQL_COUNT = "SELECT count(1) FROM {name} WHERE topic=%s AND owner_id IS NULL"

# Count tasks in all topics
SQL_COUNT_2 = "SELECT topic, count(*) FROM {name} WHERE owner_id IS NULL GROUP BY topic"


class MySQLBackend(Backend):
    def __init__(self, connection, prefix: str):
        """
        :param connection: https://peps.python.org/pep-0249/#connection-objects
        """
        super().__init__()
        self.connection = connection
        self.prefix = prefix
        self.queue_table = f"{self.prefix}_queue"
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

    def put(
        self,
        payload: bytes,
        topic: int,
        priority: int,
        delay: int,
        failure_base_delay: int,
        visibility_timeout: int,
    ) -> None:
        if len(payload) > PAYLOAD_MAX_SIZE:
            raise ValueError(
                f"payload exceeds PAYLOAD_MAX_SIZE ({len(payload)} > {PAYLOAD_MAX_SIZE})"
            )
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                SQL_INSERT.format(name=self.queue_table),
                args=(
                    payload,
                    topic,
                    priority,
                    delay,
                    failure_base_delay,
                    visibility_timeout,
                ),
            )
            self.connection.commit()

    def release_stalled_tasks(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                SQL_UPDATE.format(name=self.queue_table),
                args=(topic,),
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

    def batch_get(self, topic: int, size: int) -> List["Message"]:
        with self.connection.cursor() as cur:
            self.connection.begin()

            cur.execute(
                SQL_BATCH_SELECT.format(name=self.queue_table), args=(topic, size)
            )

            rows = cur.fetchall()
            if len(rows) == 0:
                self.connection.rollback()
                return []

            idxes = [x[0] for x in rows]
            cur.execute(
                SQL_BATCH_UPDATE.format(name=self.queue_table),
                args=(self.owner_id, idxes),
            )

            self.connection.commit()

        return [Message(x[2], x[0], self) for x in rows]

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
