from squeal import Backend, Message, QueueEmpty
from typing import List, Tuple
import time
from collections import Counter


class LocalBackend(Backend):
    """
    This is very unoptimized and is mostly intended as a reference
    implementation of the Backend interface for testing.

    Not threadsafe, either.  If you need a local queue, just use `queue` from
    the standard library.
    """

    def __init__(self):
        self.messages = []
        self.next_id = 0
        self.created = False

    def create(self) -> None:
        self.created = True

    def destroy(self) -> None:
        self.created = False

    def put(
        self, payload: bytes, topic: int, delay: int, visibility_timeout: int
    ) -> None:
        assert self.created
        self.messages.append(
            {
                "id": self.next_id,
                "payload": payload,
                "topic": topic,
                "acquired": False,
                "visibility_timeout": visibility_timeout,
                "delivery_time": time.time() + delay,
            }
        )
        self.next_id += 1

    def release_stalled_tasks(self, topic: int) -> int:
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

    def nack(self, task_id: int, delay: int) -> None:
        assert self.created
        for idx, msg in enumerate(self.messages):
            if msg["id"] != task_id:
                continue
            if not msg["acquired"]:
                continue

            msg["acquired"] = False
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

        return counts.items()

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
