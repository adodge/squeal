import random
from typing import List, Tuple, Optional, Collection, Iterable

from .base import Backend, Message, TopicLock, PUT_RECORD_COLLECTION

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
    delivery_time TIMESTAMP NOT NULL,
    failure_base_delay INT UNSIGNED NOT NULL,
    failure_count INT UNSIGNED DEFAULT 0,
    expire_time TIMESTAMP NULL,
    payload VARBINARY({size}),
    PRIMARY KEY (id),
    UNIQUE (hash),
    INDEX (topic, delivery_time)
)
"""
SQL_CREATE_LOCKS = """
CREATE TABLE IF NOT EXISTS {name} (
    topic INT UNSIGNED NOT NULL PRIMARY KEY,
    owner_id INT UNSIGNED NOT NULL,
    expire_time TIMESTAMP NOT NULL,
    INDEX (expire_time),
    INDEX (owner_id)
)
"""
SQL_CREATE_RATE_LIMITS = """
CREATE TABLE IF NOT EXISTS {name} (
    hash BINARY({key_size}) NOT NULL PRIMARY KEY,
    command_id INT UNSIGNED NOT NULL,
    expire_time TIMESTAMP NOT NULL,
    INDEX (expire_time)
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
SQL_INSERT = (
    "INSERT INTO {name} (payload, topic, hash, priority, delivery_time, failure_base_delay)"
    "VALUES (%s, %s, %s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP), %s)"
)

# Can't guarantee that columns are updated in a particular
# order in one update statement, so we do this in two steps.
SQL_BATCH_NACK_1 = """
UPDATE {name}
   SET delivery_time=TIMESTAMPADD(SECOND, failure_base_delay * POW(2, failure_count), CURRENT_TIMESTAMP)
   WHERE owner_id=%s AND id IN %s
"""
SQL_BATCH_NACK_2 = """
UPDATE {name}
   SET owner_id=NULL, failure_count = failure_count + 1
   WHERE owner_id=%s AND id IN %s
"""

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
    def __init__(
        self, connection, prefix: str, garbage_collection_interval: int = 1000
    ):
        """
        :param connection: https://peps.python.org/pep-0249/#connection-objects
        """
        self.connection = connection
        self.prefix = prefix
        self.queue_table = f"{self.prefix}_queue"
        self.lock_table = f"{self.prefix}_lock"
        self.rate_limit_table = f"{self.prefix}_limits"
        self.owner_id = random.randint(0, 2**32 - 1)
        self.garbage_collection_interval = garbage_collection_interval
        self.garbage_collection_action_count = random.randint(
            0, garbage_collection_interval - 1
        )

    def _gc_increment(self, n: int):
        self.garbage_collection_action_count += n
        if self.garbage_collection_action_count >= self.garbage_collection_interval:
            self._gc()

    def _gc(self):
        self.garbage_collection_action_count = 0
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"DELETE FROM {self.lock_table} WHERE expire_time < CURRENT_TIMESTAMP"
            )
            cur.execute(
                f"DELETE FROM {self.rate_limit_table} WHERE expire_time < CURRENT_TIMESTAMP"
            )
            self.connection.commit()

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
        data: PUT_RECORD_COLLECTION,
        priority: int,
        delay: int,
        failure_base_delay: int,
        rate_limit_seconds: Optional[int] = None,
    ) -> int:
        self.validate_hashes([x[2] for x in data])
        self.validate_payloads([x[0] for x in data])
        data = self.filter_by_rate_limit(data, rate_limit_seconds)

        no_hash_rows = [
            (payload, topic, priority, delay, failure_base_delay)
            for payload, topic, hsh in data
            if hsh is None
        ]
        hash_rows = [
            (payload, topic, hsh, priority, delay, failure_base_delay)
            for payload, topic, hsh in data
            if hsh is not None
        ]

        with self.connection.cursor() as cur:
            self.connection.begin()
            total = 0
            if no_hash_rows:
                total += cur.executemany(
                    f"INSERT INTO {self.queue_table} "
                    f"(payload, topic, priority, delivery_time, failure_base_delay) "
                    f"VALUES (%s, %s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP), %s) ",
                    args=no_hash_rows,
                )
            if hash_rows:
                total += cur.executemany(
                    f"INSERT IGNORE INTO {self.queue_table} "
                    f"(payload, topic, hash, priority, delivery_time, failure_base_delay) "
                    f"VALUES (%s, %s, %s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP), %s)",
                    args=hash_rows,
                )
            self.connection.commit()
        return total

    def batch_get(
        self, topic: int, size: int, visibility_timeout: int
    ) -> List["Message"]:
        with self.connection.cursor() as cur:
            self.connection.begin()

            cur.execute(
                f"""
                SELECT id, owner_id, payload FROM {self.queue_table}
                    WHERE (owner_id IS NULL OR expire_time < CURRENT_TIMESTAMP)
                        AND topic=%s AND CURRENT_TIMESTAMP >= delivery_time
                    ORDER BY priority DESC, id ASC
                    LIMIT %s FOR UPDATE SKIP LOCKED;
                """,
                args=(topic, size),
            )

            rows = cur.fetchall()
            if len(rows) == 0:
                self.connection.rollback()
                return []

            idxes = [x[0] for x in rows]
            cur.execute(
                f"UPDATE {self.queue_table} "
                f"SET owner_id=%s, expire_time=TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP) "
                f"WHERE id IN %s",
                args=(self.owner_id, visibility_timeout, idxes),
            )

            self.connection.commit()

        return [
            Message(x[2], x[0], self, visibility_timeout=visibility_timeout)
            for x in rows
        ]

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

    def batch_touch(self, task_ids: Collection[int], visibility_timeout: int) -> None:
        if len(task_ids) == 0:
            return
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"""
                UPDATE {self.queue_table}
                   SET expire_time = TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP)
                   WHERE owner_id=%s AND id IN %s
                """,
                args=(visibility_timeout, self.owner_id, list(task_ids)),
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
        self._gc_increment(1)
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(SQL_LIST_TOPICS.format(name=self.queue_table))
            topics = list(cur.fetchall())

            topics.sort(key=lambda x: -x[1])
            new_lock = None
            for topic_id, _ in topics:
                result = cur.execute(
                    f"INSERT INTO {self.lock_table} "
                    f"(topic, owner_id, expire_time) "
                    f"VALUES (%s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP)) as new(top,new_owner,new_expire) "
                    f"ON DUPLICATE KEY UPDATE "
                    f"owner_id=IF(expire_time<CURRENT_TIMESTAMP, new_owner, owner_id), "
                    f"expire_time=IF(expire_time<CURRENT_TIMESTAMP, new_expire, expire_time) ",
                    args=(topic_id, self.owner_id, topic_lock_visibility_timeout),
                )
                if result == 0:  # set to current values
                    continue
                else:
                    new_lock = topic_id
                    break

            self.connection.commit()

            if new_lock is not None:
                return TopicLock(new_lock, self, topic_lock_visibility_timeout)
            return None

    def batch_release_topic(self, topics: Collection[int]) -> None:
        if len(topics) == 0:
            return
        self._gc_increment(len(topics))
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"""
            DELETE FROM {self.lock_table} WHERE topic IN %s AND owner_id = %s
            """,
                args=(list(topics), self.owner_id),
            )
            self.connection.commit()

    def batch_touch_topic(
        self, topics: Collection[int], topic_lock_visibility_timeout: int
    ) -> None:
        if len(topics) == 0:
            return
        self._gc_increment(len(topics))
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(
                f"""
            UPDATE {self.lock_table} SET expire_time=TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP)
            WHERE topic IN %s AND owner_id = %s
            """,
                args=(topic_lock_visibility_timeout, list(topics), self.owner_id),
            )
            self.connection.commit()

    def rate_limit(
        self, hshes: Collection[bytes], interval_seconds: int
    ) -> List[bytes]:
        if not hshes:
            return []
        self.validate_hashes(hshes)
        self._gc_increment(len(hshes))

        command_id = random.randint(0, 2**32 - 1)

        with self.connection.cursor() as cur:
            self.connection.begin()
            input_rows = [(hsh, command_id, interval_seconds) for hsh in hshes]
            n_rows = cur.executemany(
                f"INSERT INTO {self.rate_limit_table} "
                f"(hash, command_id, expire_time) VALUES "
                f"  (%s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP)) as new(a,b,c) "
                f"ON DUPLICATE KEY UPDATE "
                f"  command_id=IF(expire_time < CURRENT_TIMESTAMP, b, command_id), "
                f"  expire_time=IF(expire_time < CURRENT_TIMESTAMP, c, expire_time)",
                args=input_rows,
            )
            self.connection.commit()
            if n_rows == 0:
                return []
            cur.execute(
                f"SELECT hash FROM {self.rate_limit_table} WHERE hash IN %s AND command_id=%s",
                args=(list(hshes), command_id),
            )
            res = list([x[0] for x in cur.fetchall()])
            return res

    def override_rate_limit(
        self, hshes: Collection[bytes], interval_seconds: int
    ) -> None:
        if not hshes:
            return
        self.validate_hashes(hshes)
        self._gc_increment(len(hshes))

        if interval_seconds > 0:
            with self.connection.cursor() as cur:
                self.connection.begin()
                cur.executemany(
                    f"INSERT INTO {self.rate_limit_table} "
                    f"(hash, command_id, expire_time) VALUES"
                    f"  (%s, %s, TIMESTAMPADD(SECOND, %s, CURRENT_TIMESTAMP)) as new(a,b,c) "
                    f"ON DUPLICATE KEY UPDATE "
                    f"expire_time=c, command_id=b",
                    args=[(hsh, 0, interval_seconds) for hsh in hshes],
                )
                self.connection.commit()
        else:
            with self.connection.cursor() as cur:
                self.connection.begin()
                cur.execute(
                    f"DELETE FROM {self.rate_limit_table} " f"WHERE hash IN %s",
                    args=list([(x,) for x in hshes]),
                )
                self.connection.commit()
