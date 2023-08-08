import abc
import random
from typing import Tuple, Type, Union, List


class QueueEmpty(BaseException):
    pass


class Backend(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    def create(self) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        raise NotImplementedError

    def put(self, item: bytes, topic: int) -> None:
        raise NotImplementedError

    def release_stalled_tasks(self, topic: int) -> int:
        raise NotImplementedError

    def get(self, topic: int) -> "Task":
        raise NotImplementedError

    def ack(self, task_id: int) -> None:
        raise NotImplementedError

    def nack(self, task_id: int) -> None:
        raise NotImplementedError

    def topics(self) -> List[Tuple[int, int]]:
        raise NotImplementedError

    def size(self, topic: int) -> int:
        raise NotImplementedError


class Task:
    def __init__(self, payload: bytes, idx: int, backend: Backend):
        self.payload = payload
        self.idx = idx
        self.backend = backend
        self.status = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.status is None:
            self.nack()

    def ack(self):
        if self.status is not None:
            raise RuntimeError("Trying to ack a Task that's already been acked or nacked")
        self.status = True
        self.backend.ack(self.idx)

    def nack(self):
        if self.status is not None:
            raise RuntimeError("Trying to nack a Task that's already been acked or nacked")
        self.status = False
        self.backend.nack(self.idx)


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
        self.owner_id = random.randint(0, 2 ** 32 - 1)

    def create(self) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.queue_table} (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            topic INT UNSIGNED NOT NULL,
            owner_id INT UNSIGNED NULL,
            acquire_time DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
            payload VARBINARY(1024),
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
                args=(item, topic))
            self.connection.commit()

    def release_stalled_tasks(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(f"""
                UPDATE {self.queue_table} SET owner_id=NULL
                WHERE owner_id IS NOT NULL AND topic=%s
                    AND TIMESTAMPDIFF(SECOND, acquire_time, NOW()) > %s
            """, args=(topic, self.acquire_timeout))
            rows = cur.rowcount
            self.connection.commit()
            return rows

    def get(self, topic: int) -> "Task":
        with self.connection.cursor() as cur:
            self.connection.begin()

            cur.execute(f"""
                SELECT id, owner_id, payload FROM {self.queue_table}
                    WHERE owner_id IS NULL AND topic=%s
                    ORDER BY id
                    LIMIT 1 FOR UPDATE SKIP LOCKED;
            """, args=(topic,))

            row = cur.fetchone()
            if row is None:
                self.connection.rollback()
                raise QueueEmpty()

            cur.execute(f"UPDATE {self.queue_table} SET owner_id={self.owner_id} WHERE id=%s",
                        args=(row[0],))

            self.connection.commit()

        return Task(row[2], row[0], self)

    def ack(self, task_id: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(f"DELETE FROM {self.queue_table} WHERE id=%s",
                        args=(task_id,))
            self.connection.commit()

    def nack(self, task_id: int) -> None:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(f"UPDATE {self.queue_table} SET owner_id=NULL WHERE id=%s AND owner_id=%s",
                        args=(task_id, self.owner_id))
            self.connection.commit()

    def size(self, topic: int) -> int:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(f"SELECT count(*) FROM {self.queue_table} WHERE topic=%s AND owner_id IS NULL",
                        args=(topic,))
            result = cur.fetchone()
            self.connection.commit()
            return result[0]

    def topics(self) -> List[Tuple[int, int]]:
        with self.connection.cursor() as cur:
            self.connection.begin()
            cur.execute(f"SELECT topic, count(*) FROM {self.queue_table} "
                        f"WHERE owner_id IS NULL GROUP BY topic")
            rows = cur.fetchall()
            self.connection.commit()
        return rows


class SQLQueue:
    """
    FIFO(-ish) queue backed by a SQL table
    """

    def __init__(self, backend_instance_or_class: Union[Backend, Type[Backend]],
                 *backend_args, **backend_kwargs):
        if isinstance(backend_instance_or_class, Backend):
            self.backend = backend_instance_or_class
            if backend_args or backend_kwargs:
                raise RuntimeError("If you provide a Backend instance, you cannot also provide arguments")
        elif issubclass(backend_instance_or_class, Backend):
            self.backend = backend_instance_or_class(*backend_args, **backend_kwargs)
        else:
            raise RuntimeError("Must provide either am Backend instance or a class and arguments")

    def create(self) -> None:
        self.backend.create()

    def destroy(self) -> None:
        self.backend.destroy()

    def put(self, item: bytes, topic: int) -> None:
        self.backend.put(item, topic)

    def get(self, topic: int) -> "Task":
        try:
            return self.backend.get(topic)
        except QueueEmpty:
            if self.backend.release_stalled_tasks(topic) == 0:
                raise QueueEmpty()
        return self.backend.get(topic)

    def topics(self) -> List[Tuple[int, int]]:
        return self.backend.topics()

    def size(self, topic: int) -> int:
        return self.backend.size(topic)

    def ack(self, task_id: int) -> None:
        self.backend.ack(task_id)

    def nack(self, task_id: int) -> None:
        self.backend.nack(task_id)


class SQLMonoQueue(SQLQueue):
    """
    FIFO(-ish) queue backed by a SQL table, with only one topic
    """

    def __init__(self, backend_instance_or_class: Union[Backend, Type[Backend]],
                 *backend_args, **backend_kwargs):
        self.topic = backend_kwargs.pop('topic', 0)
        super().__init__(backend_instance_or_class, *backend_args, **backend_kwargs)

    def put(self, item: bytes, topic=None) -> None:
        if topic is not None:
            raise RuntimeError("Trying to use a MonoQueue with an explicit topic.")
        super().put(item, self.topic)

    def get(self, topic=None) -> "Task":
        if topic is not None:
            raise RuntimeError("Trying to use a MonoQueue with an explicit topic.")
        return super().get(self.topic)

    def topics(self) -> List[Tuple[int, int]]:
        raise RuntimeError("Trying to query topics on MonoQueue")

    def size(self, topic=None) -> int:
        if topic is not None:
            raise RuntimeError("Trying to use a MonoQueue with an explicit topic.")
        return super().size(self.topic)


__all__ = ["SQLQueue", "SQLMonoQueue", "MySQLBackend", "Task", "QueueEmpty"]
