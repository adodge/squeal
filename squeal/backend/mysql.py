import random
from typing import List, Tuple, Optional, Collection

from .base import Backend, Message, TopicLock

# Create a table to store queue items
# format args:
#   name -> table name
#   hash_size -> hsh size (bytes)
#   size -> max message size (bytes)
SQL_CREATE = """
CREATE TABLE IF NOT EXISTS {name} (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    topic INT UNSIGNED NOT NULL,
    hash BINARY({hash_size}) NULL,
    priority INT UNSIGNED NOT NULL,
    owner_id INT UNSIGNED NULL,
    delivery_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    visibility_timeout INT UNSIGNED NOT NULL,
    failure_base_delay INT UNSIGNED NOT NULL,
    failure_count INT UNSIGNED DEFAULT 0,
    acquire_time DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    payload VARBINARY({size}),
    PRIMARY KEY (id),
    UNIQUE (topic, hash)
)
"""
SQL_CREATE_LOCKS = """
CREATE TABLE IF NOT EXISTS {name} (
    topic INT UNSIGNED NOT NULL,
    owner_id INT UNSIGNED NOT NULL,
    visibility_timeout INT UNSIGNED NOT NULL,
    acquire_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic)
)
"""
SQL_CREATE_RATE_LIMITS = """
CREATE TABLE IF NOT EXISTS {name} (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    hash BINARY({key_size}) NULL,
    expire_time DATETIME NOT NULL,
    PRIMARY KEY (id),
    INDEX (hash)
)
"""

# Destroy the table
# format args:
#   name -> table name
SQL_DROP = "DROP TABLE IF EXISTS {name}"

# Insert a message into the queue
# format args:
#   name -> table name
# sql substitution args:
# * payload
# * topic
# * hsh
# * priority
# * delay (seconds)
# * failure_base_delay (seconds)
# * visibility timeout (seconds)
SQL_INSERT = (
    "INSERT INTO {name} (payload, topic, hash, priority, delivery_time, failure_base_delay, visibility_timeout)"
    "VALUES (%s, %s, %s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP), %s, %s)"
)

# Release stalled messages
# format args:
#   name -> table name
SQL_UPDATE = """
UPDATE {name} SET owner_id=NULL
    WHERE owner_id IS NOT NULL
    AND TIMESTAMPDIFF(SECOND, acquire_time, CURRENT_TIMESTAMP) > visibility_timeout
"""

# Can't guarantee that columns are updated in a particular
# order in one update statement, so we do this in two steps.
SQL_BATCH_NACK_1 = """
UPDATE {name}
   SET delivery_time=TIMESTAMPADD(SECOND, failure_base_delay * POW(2, failure_count), CURRENT_TIMESTAMP)
   WHERE owner_id=%s AND id IN %s
"""
SQL_BATCH_NACK_2 = """
UPDATE {name}
   SET owner_id=NULL,
       failure_count = failure_count + 1
   WHERE owner_id=%s AND id IN %s
"""

# Refresh the acquire time for a bunch of messages
SQL_BATCH_TOUCH = """
UPDATE {name}
   SET acquire_time = CURRENT_TIMESTAMP
   WHERE owner_id=%s AND id IN %s
"""

SQL_BATCH_SELECT = """
SELECT id, owner_id, payload FROM {name}
    WHERE owner_id IS NULL AND topic=%s AND TIMESTAMPDIFF(SECOND, delivery_time, CURRENT_TIMESTAMP) >= 0
    ORDER BY priority DESC, id ASC
    LIMIT %s FOR UPDATE SKIP LOCKED;
"""
SQL_BATCH_UPDATE = "UPDATE {name} SET owner_id=%s WHERE id IN %s"

# Finish a message
SQL_ACK = "DELETE FROM {name} WHERE id=%s"

# Count messages in topic
SQL_GET_TOPIC_SIZE = "SELECT count(1) FROM {name} WHERE topic=%s AND owner_id IS NULL"

# Count messages in all topics
SQL_LIST_TOPICS = """
SELECT topic, count(*) FROM {name}
    WHERE owner_id IS NULL AND TIMESTAMPDIFF(SECOND, delivery_time, CURRENT_TIMESTAMP) >= 0
    GROUP BY topic
"""


class MySQLBackend(Backend):
    def __init__(self, connection, prefix: str):
        """
        :param connection: https://peps.python.org/pep-0249/#connection-objects
        """
        super().__init__()
        self.connection = connection
        self.prefix = prefix
        self.queue_table = f"{self.prefix}_queue"
        self.lock_table = f"{self.prefix}_lock"
        self.rate_limit_table = f"{self.prefix}_limits"
        self.owner_id = random.randint(0, 2**32 - 1)

    @property
    def max_payload_size(self) -> Optional[int]:
        return 2047

    @property
    def hash_size(self) -> int:
        return 16

    def create(self) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                SQL_CREATE.format(
                    name=self.queue_table,
                    size=self.max_payload_size,
                    hash_size=self.hash_size,
                )
            )
            cur.execute(SQL_CREATE_LOCKS.format(name=self.lock_table))
            cur.execute(
                SQL_CREATE_RATE_LIMITS.format(
                    name=self.rate_limit_table, key_size=self.hash_size
                )
            )
            self.connection.commit()

    def destroy(self) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_DROP.format(name=self.queue_table))
            cur.execute(SQL_DROP.format(name=self.lock_table))
            cur.execute(SQL_DROP.format(name=self.rate_limit_table))
            self.connection.commit()

    def batch_put(
        self,
        data: Collection[Tuple[bytes, int, Optional[bytes]]],
        priority: int,
        delay: int,
        failure_base_delay: int,
        visibility_timeout: int,
    ) -> int:
        for payload, topic, hsh in data:
            if len(payload) > self.max_payload_size:
                raise ValueError(
                    f"payload exceeds PAYLOAD_MAX_SIZE ({len(payload)} > {self.max_payload_size})"
                )
            if hsh is not None and len(hsh) != self.hash_size:
                raise ValueError(
                    f"hsh size is not HASH_SIZE ({len(hsh)} != {self.hash_size})"
                )

        with self.connection.cursor() as cur:
            self.connection.begin()
            rows = [
                (
                    payload,
                    topic,
                    hsh,
                    priority,
                    delay,
                    failure_base_delay,
                    visibility_timeout,
                )
                for payload, topic, hsh in data
            ]
            total = 0
            for row in rows:
                try:
                    cur.execute(
                        SQL_INSERT.format(name=self.queue_table),
                        args=row,
                    )
                    total += cur.rowcount
                except Exception as err:
                    if err.__class__.__name__ != "IntegrityError":
                        raise
            self.connection.commit()
            return total

    def release_stalled_messages(self) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_UPDATE.format(name=self.queue_table))
            rows = cur.rowcount
            self.connection.commit()
            return rows

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
            cur.execute(SQL_ACK.format(name=self.queue_table), args=(task_id,))
            # TODO raise if it's already expired
            self.connection.commit()

    def batch_nack(self, task_ids: Collection[int]) -> None:
        if len(task_ids) == 0:
            return
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                SQL_BATCH_NACK_1.format(name=self.queue_table),
                args=(self.owner_id, list(task_ids)),
            )
            cur.execute(
                SQL_BATCH_NACK_2.format(name=self.queue_table),
                args=(self.owner_id, list(task_ids)),
            )
            # TODO raise if it's already expired
            self.connection.commit()

    def batch_touch(self, task_ids: Collection[int]) -> None:
        if len(task_ids) == 0:
            return
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                SQL_BATCH_TOUCH.format(name=self.queue_table),
                args=(self.owner_id, list(task_ids)),
            )
            # TODO raise if it's already expired
            self.connection.commit()

    def get_topic_size(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_GET_TOPIC_SIZE.format(name=self.queue_table), args=(topic,))
            result = cur.fetchone()
            self.connection.commit()
            return result[0]

    def list_topics(self) -> List[Tuple[int, int]]:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_LIST_TOPICS.format(name=self.queue_table))
            rows = cur.fetchall()
            self.connection.commit()
        return rows

    def acquire_topic(
        self, topic_lock_visibility_timeout: int
    ) -> Optional["TopicLock"]:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_LIST_TOPICS.format(name=self.queue_table))
            topics = list(cur.fetchall())
            cur.execute(f"LOCK TABLE {self.lock_table} WRITE")

            topics.sort(key=lambda x: -x[1])
            new_lock = None
            for topic, count in topics:
                try:
                    cur.execute(
                        f"INSERT INTO {self.lock_table} (topic, owner_id, visibility_timeout) VALUES (%s, %s, %s)",
                        args=(topic, self.owner_id, topic_lock_visibility_timeout),
                    )
                except Exception:  # XXX
                    continue
                new_lock = topic
                break

            self.connection.commit()

            if new_lock is not None:
                return TopicLock(new_lock, self)
            return None

    def batch_release_topic(self, topics: Collection[int]) -> None:
        if len(topics) == 0:
            return
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"""
            DELETE FROM {self.lock_table} WHERE topic IN %s AND owner_id = %s
            """,
                args=(list(topics), self.owner_id),
            )
            self.connection.commit()

    def batch_touch_topic(self, topics: Collection[int]) -> None:
        if len(topics) == 0:
            return
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"""
            UPDATE {self.lock_table} SET acquire_time=CURRENT_TIMESTAMP
            WHERE topic IN %s AND owner_id = %s
            """,
                args=(list(topics), self.owner_id),
            )
            self.connection.commit()

    def release_stalled_topic_locks(self) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"""
            DELETE FROM {self.lock_table} WHERE
            TIMESTAMPDIFF(SECOND, acquire_time, CURRENT_TIMESTAMP) > visibility_timeout
            """
            )
            rows = cur.rowcount
            self.connection.commit()
            return rows

    def rate_limit(self, key: bytes, max_events: int, interval_seconds: int) -> bool:
        if len(key) != self.hash_size:
            raise ValueError(
                f"rate limit key size is not HASH_SIZE ({len(key)} != {self.hash_size})"
            )
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"DELETE FROM {self.rate_limit_table} "
                f"WHERE expire_time < CURRENT_TIMESTAMP"
            )
            cur.execute(
                f"SELECT COUNT(*) FROM {self.rate_limit_table} WHERE hash=%s",
                args=(key,),
            )
            (n,) = cur.fetchone()
            if n >= max_events:
                self.connection.commit()
                return False
            cur.execute(
                f"INSERT INTO {self.rate_limit_table} "
                f"SET hash=%s, expire_time=TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP)",
                args=(key, interval_seconds),
            )
            self.connection.commit()
            return True
