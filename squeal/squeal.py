import abc
import time
from typing import Tuple, List, Optional


class QueueEmpty(BaseException):
    pass


class Backend(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    def create(self) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        raise NotImplementedError

    def put(self, item: bytes, topic: int, delay: int, visibility_timeout: int) -> None:
        raise NotImplementedError

    def release_stalled_tasks(self, topic: int) -> int:
        raise NotImplementedError

    def get(self, topic: int) -> "Message":
        raise NotImplementedError

    def ack(self, task_id: int) -> None:
        raise NotImplementedError

    def nack(self, task_id: int, delay: int) -> None:
        raise NotImplementedError

    def topics(self) -> List[Tuple[int, int]]:
        raise NotImplementedError

    def size(self, topic: int) -> int:
        raise NotImplementedError


class Message:
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
            raise RuntimeError(
                "Trying to ack a Message that's already been acked or nacked"
            )
        self.status = True
        self.backend.ack(self.idx)

    def nack(self):
        if self.status is not None:
            raise RuntimeError(
                "Trying to nack a Message that's already been acked or nacked"
            )
        self.status = False
        self.backend.nack(self.idx, delay=0)  # TODO parameterize this delay


class Queue:
    """
    FIFO(-ish) queue backed by a SQL table
    """

    def __init__(
        self,
        backend: Backend,
        timeout: int = -1,
        poll_interval: float = 1,
        delay: int = 0,
        visibility_timeout: int = 60,
        auto_create: bool = True,
    ):
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.delay = delay
        self.visibility_timeout = visibility_timeout

        self.backend = backend

        if self.poll_interval <= 0:
            raise RuntimeError("Poll interval must be positive")
        if self.delay < 0:
            raise RuntimeError("Poll interval must be non-negative")
        if self.visibility_timeout <= 0:
            raise RuntimeError("Visibility timeout must be positive")

        if auto_create:
            self.create()

    def create(self) -> None:
        self.backend.create()

    def destroy(self) -> None:
        self.backend.destroy()

    def put(self, item: bytes, topic: int) -> None:
        self.backend.put(item, topic, self.delay, self.visibility_timeout)

    def get_nowait(self, topic: int) -> "Message":
        try:
            return self.backend.get(topic)
        except QueueEmpty:
            if self.backend.release_stalled_tasks(topic) == 0:
                raise QueueEmpty()
        return self.backend.get(topic)

    def get(
        self,
        topic: int,
        timeout: Optional[int] = None,
    ) -> "Message":
        if timeout is None:
            timeout = self.timeout

        if timeout == 0:
            return self.get_nowait(topic)

        t0 = time.time()
        while True:
            t1 = time.time()
            if timeout > 0 and timeout <= t1 - t0:
                raise QueueEmpty()
            try:
                return self.get_nowait(topic)
            except QueueEmpty:
                pass
            time.sleep(self.poll_interval)

    def topics(self) -> List[Tuple[int, int]]:
        return self.backend.topics()

    def size(self, topic: int) -> int:
        return self.backend.size(topic)


class MonoQueue(Queue):
    """
    FIFO(-ish) queue backed by a SQL table, with only one topic
    """

    @staticmethod
    def maybe_raise_superfluous_topic(topic: Optional[int]):
        if topic is not None:
            raise RuntimeError(
                "Trying to call a MonoQueue method with an explicit topic"
            )

    def __init__(self, *args, topic: int = 0, **kwargs):
        self.topic = topic
        super().__init__(*args, **kwargs)

    def put(self, item: bytes, topic=None) -> None:
        self.maybe_raise_superfluous_topic(topic)
        super().put(item, self.topic)

    def get_nowait(self, topic=None) -> "Message":
        self.maybe_raise_superfluous_topic(topic)
        return super().get_nowait(self.topic)

    def get(
        self,
        topic=None,
        timeout: Optional[int] = None,
        poll_interval: Optional[int] = None,
    ) -> "Message":
        self.maybe_raise_superfluous_topic(topic)
        return super().get(
            topic=self.topic, timeout=timeout, poll_interval=poll_interval
        )

    def topics(self) -> List[Tuple[int, int]]:
        raise NotImplementedError

    def size(self, topic=None) -> int:
        self.maybe_raise_superfluous_topic(topic)
        return super().size(self.topic)


__all__ = ["Queue", "MonoQueue", "Message", "QueueEmpty", "Backend"]
