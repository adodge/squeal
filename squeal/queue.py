import time
from typing import Tuple, List, Sequence, Dict, Iterable, Optional
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
        self.backend = backend
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.new_message_delay = new_message_delay
        self.failure_base_delay = failure_base_delay
        self.visibility_timeout = visibility_timeout

        self.messages: Dict[int, "Message"] = {}

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

    def put(
        self, item: bytes, topic: int, priority: int = 0, hsh: Optional[bytes] = None
    ) -> None:
        self.backend.put(
            item,
            topic,
            hsh,
            priority,
            self.new_message_delay,
            self.failure_base_delay,
            self.visibility_timeout,
        )

    def _get_nowait(self, topic: int) -> "Message":
        try:
            return self.backend.get(topic)
        except QueueEmpty:
            if self.backend.release_stalled_messages(topic) == 0:
                raise QueueEmpty()
        return self.backend.get(topic)

    def get_nowait(self, topic: int) -> "Message":
        msg = self._get_nowait(topic)
        self.messages[msg.idx] = msg
        return msg

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

    def batch_get(self, topics: Iterable[Tuple[int, int]]) -> List["Message"]:
        out = []
        for topic, size in topics:
            if size <= 0:
                continue
            topic_msgs = self.backend.batch_get(topic, size)
            if len(topic_msgs) < size:
                if self.backend.release_stalled_messages(topic) > 0:
                    topic_msgs.extend(
                        self.backend.batch_get(topic, size - len(topic_msgs))
                    )
            out.extend(topic_msgs)
        for msg in out:
            self.messages[msg.idx] = msg
        return out

    def _prune_stored_messages(self):
        released = [msg.idx for msg in self.messages.values() if msg.released]
        for idx in released:
            del self.messages[idx]

    def touch(self) -> None:
        self._prune_stored_messages()
        self.backend.batch_touch(self.messages.keys())

    def nack(self) -> None:
        self._prune_stored_messages()
        self.backend.batch_nack(self.messages.keys())
        self.messages.clear()

    def topics(self) -> List[Tuple[int, int]]:
        return self.backend.topics()

    def size(self, topic: int) -> int:
        return self.backend.size(topic)


__all__ = ["Queue"]
