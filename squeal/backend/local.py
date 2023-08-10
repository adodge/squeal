from .base import Backend, Message, QueueEmpty
from typing import List, Tuple
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
        self.next_id = 0
        self.created = False

    def create(self) -> None:
        self.created = True

    def destroy(self) -> None:
        self.created = False

    def put(
        self,
        payload: bytes,
        topic: int,
        priority: int,
        delay: int,
        failure_base_delay: int,
        visibility_timeout: int,
    ) -> None:
        assert self.created
        self.messages.append(
            {
                "id": self.next_id,
                "payload": payload,
                "topic": topic,
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
            msg["failure_count"] += 1
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
        self.messages.remove(idx)

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
