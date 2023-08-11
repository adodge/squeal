from .base import Backend, Message, QueueEmpty
from typing import List, Tuple, Iterable, Optional
import time
from collections import Counter


class LocalBackend(Backend):
    """
    This is just a reference implementation for testing purposes.  Don't use it for anything else.  It's deliberately
    unoptimized to make it easy to follow what it's doing.
    """

    def __init__(self):
        super().__init__()
        self.messages = []
        self.unique_constraint = set()
        self.next_id = 0
        self.created = False
        self._max_payload_size = None
        self._hash_size = 16

    @property
    def max_payload_size(self) -> Optional[int]:
        return self._max_payload_size

    @property
    def hash_size(self) -> int:
        return self._hash_size

    def create(self) -> None:
        self.created = True

    def destroy(self) -> None:
        self.created = False

    def put(
        self,
        payload: bytes,
        topic: int,
        hsh: Optional[bytes],
        priority: int,
        delay: int,
        failure_base_delay: int,
        visibility_timeout: int,
    ) -> None:
        assert self.created
        if hsh is not None and len(hsh) != self._hash_size:
            raise ValueError(
                f"hsh size is not HASH_SIZE ({len(hsh)} != {self._hash_size})"
            )

        if hsh is not None:
            if (topic, hsh) in self.unique_constraint:
                raise ValueError((topic, hsh))
            self.unique_constraint.add((topic, hsh))

        self.messages.append(
            {
                "id": self.next_id,
                "payload": payload,
                "topic": topic,
                "hsh": hsh,
                "priority": priority,
                "acquired": False,
                "visibility_timeout": visibility_timeout,
                "delivery_time": time.time() + delay,
                "failure_base_delay": failure_base_delay,
                "failure_count": 0,
            }
        )
        self.next_id += 1

    def release_stalled_messages(self, topic: int) -> int:
        assert self.created
        now = time.time()
        n = 0
        for msg in self.messages:
            if msg["topic"] != topic:
                continue
            if not msg["acquired"]:
                continue
            if msg["visibility_timeout"] + msg["acquire_time"] > now:
                continue

            msg["acquired"] = False
            n += 1
        return n

    def get(self, topic: int) -> "Message":
        assert self.created
        now = time.time()
        self.messages.sort(key=lambda x: [-x["priority"], x["id"]])
        for msg in self.messages:
            if msg["acquired"]:
                continue
            if msg["topic"] != topic:
                continue
            if msg["delivery_time"] > now:
                continue

            msg["acquired"] = True
            msg["acquire_time"] = now
            return Message(msg["payload"], msg["id"], self)

        raise QueueEmpty()

    def batch_get(self, topic: int, size: int) -> List["Message"]:
        assert self.created
        output = []
        while len(output) < size:
            try:
                output.append(self.get(topic))
            except QueueEmpty:
                break
        return output

    def ack(self, task_id: int) -> None:
        assert self.created
        for idx, msg in enumerate(self.messages):
            if msg["id"] != task_id:
                continue
            if not msg["acquired"]:
                continue
            break
        else:
            return
        if self.messages[idx]["hsh"] is not None:
            k = (self.messages[idx]["topic"], self.messages[idx]["hsh"])
            self.unique_constraint.remove(k)
        del self.messages[idx]

    def nack(self, task_id: int) -> None:
        assert self.created
        for idx, msg in enumerate(self.messages):
            if msg["id"] != task_id:
                continue
            if not msg["acquired"]:
                continue

            msg["acquired"] = False
            delay = msg["failure_base_delay"] * (2 ** msg["failure_count"])
            msg["failure_count"] += 1
            msg["delivery_time"] = time.time() + delay

            break

    def batch_nack(self, task_ids: Iterable[int]) -> None:
        assert self.created
        for task_id in task_ids:
            self.nack(task_id)

    def touch(self, task_id: int) -> None:
        assert self.created
        for msg in self.messages:
            if msg["id"] != task_id:
                continue
            if not msg["acquired"]:
                continue
            msg["acquire_time"] = time.time()
            break

    def batch_touch(self, task_ids: Iterable[int]) -> None:
        assert self.created
        for task_id in task_ids:
            self.touch(task_id)

    def topics(self) -> List[Tuple[int, int]]:
        assert self.created
        counts = Counter()

        now = time.time()
        for msg in self.messages:
            if msg["acquired"]:
                continue
            if msg["delivery_time"] > now:
                continue

            counts[msg["topic"]] += 1

        return list(counts.items())

    def size(self, topic: int) -> int:
        assert self.created
        n = 0
        now = time.time()
        for msg in self.messages:
            if msg["acquired"]:
                continue
            if msg["topic"] != topic:
                continue
            if msg["delivery_time"] > now:
                continue

            n += 1
        return n
