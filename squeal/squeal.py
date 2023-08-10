import time
from typing import Tuple, List
from squeal.backend.base import Backend, Message, QueueEmpty


class Queue:
    """
    FIFO(-ish) queue backed by a SQL table
    """

    def __init__(
        self,
        backend: Backend,
        timeout: int = -1,
        poll_interval: float = 1,
        new_message_delay: int = 0,
        failure_base_delay: int = 1,
        visibility_timeout: int = 60,
        auto_create: bool = True,
    ):
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.new_message_delay = new_message_delay
        self.failure_base_delay = failure_base_delay
        self.visibility_timeout = visibility_timeout

        self.backend = backend

        if self.poll_interval <= 0:
            raise RuntimeError("Poll interval must be positive")
        if self.new_message_delay < 0:
            raise RuntimeError("Delay must be non-negative")
        if self.failure_base_delay < 0:
            raise RuntimeError("Delay must be non-negative")
        if self.visibility_timeout <= 0:
            raise RuntimeError("Visibility timeout must be positive")

        if auto_create:
            self.create()

    def create(self) -> None:
        self.backend.create()

    def destroy(self) -> None:
        self.backend.destroy()

    def put(self, item: bytes, topic: int, priority: int = 0) -> None:
        self.backend.put(
            item,
            topic,
            priority,
            self.new_message_delay,
            self.failure_base_delay,
            self.visibility_timeout,
        )

    def get_nowait(self, topic: int) -> "Message":
        try:
            return self.backend.get(topic)
        except QueueEmpty:
            if self.backend.release_stalled_messages(topic) == 0:
                raise QueueEmpty()
        return self.backend.get(topic)

    def get(self, topic: int) -> "Message":
        if self.timeout == 0:
            return self.get_nowait(topic)

        never_timeout = self.timeout < 0

        t0 = time.time()
        while True:
            t1 = time.time()
            if not never_timeout and self.timeout <= t1 - t0:
                raise QueueEmpty()
            try:
                return self.get_nowait(topic)
            except QueueEmpty:
                pass
            time.sleep(self.poll_interval)

    def batch_get_nowait(self, topic: int, size: int) -> List["Message"]:
        out = self.backend.batch_get(topic, size)
        if len(out) < size:
            if self.backend.release_stalled_messages(topic) == 0:
                return out
        out.extend(self.backend.batch_get(topic, size - len(out)))
        return out

    def batch_get(self, topic: int, size: int) -> List["Message"]:
        if self.timeout == 0:
            return self.batch_get_nowait(topic, size)

        never_timeout = self.timeout < 0
        out = []

        t0 = time.time()
        while True:
            t1 = time.time()
            if not never_timeout and self.timeout <= t1 - t0:
                return out
            out.extend(self.batch_get_nowait(topic, size - len(out)))
            if len(out) == size:
                return out
            time.sleep(self.poll_interval)

    def topics(self) -> List[Tuple[int, int]]:
        return self.backend.topics()

    def size(self, topic: int) -> int:
        return self.backend.size(topic)


__all__ = ["Queue"]
