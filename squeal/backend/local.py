import time
from collections import Counter
from typing import List, Tuple, Iterable, Optional, Collection

from .base import Backend, Message, TopicLock


class LocalBackend(Backend):
    """
    This is just a reference implementation for testing purposes.  Don't use it for anything else.  It's deliberately
    unoptimized to make it easy to follow what it's doing.
    """

    def __init__(self):
        super().__init__()
        self.messages = []
        self.topic_locks = {}
        self.rate_limits = {}
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

    def batch_put(
        self,
        data: Collection[Tuple[bytes, int, Optional[bytes]]],
        priority: int,
        delay: int,
        failure_base_delay: int,
        visibility_timeout: int,
    ) -> int:
        assert self.created
        for payload, topic, hsh in data:
            if hsh is not None and len(hsh) != self.hash_size:
                raise ValueError(
                    f"hsh size is not HASH_SIZE ({len(hsh)} != {self.hash_size})"
                )

        constraint_violations = []
        for payload, topic, hsh in data:
            if hsh is not None:
                if (topic, hsh) in self.unique_constraint:
                    constraint_violations.append((payload, topic, hsh))
                    continue
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

        return len(data) - len(constraint_violations)

    def release_stalled_messages(self) -> int:
        assert self.created
        now = time.time()
        n = 0
        for msg in self.messages:
            if not msg["acquired"]:
                continue
            if msg["visibility_timeout"] + msg["acquire_time"] > now:
                continue

            msg["acquired"] = False
            n += 1
        return n

    def batch_get(self, topic: int, size: int) -> List["Message"]:
        assert self.created
        now = time.time()
        self.messages.sort(key=lambda x: [-x["priority"], x["id"]])

        output = []
        for msg in self.messages:
            if len(output) >= size:
                break
            if msg["acquired"]:
                continue
            if msg["topic"] != topic:
                continue
            if msg["delivery_time"] > now:
                continue

            msg["acquired"] = True
            msg["acquire_time"] = now
            output.append(Message(msg["payload"], msg["id"], self))
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

    def batch_nack(self, task_ids: Collection[int]) -> None:
        assert self.created
        to_nack = set(task_ids)
        for idx, msg in enumerate(self.messages):
            if msg["id"] not in to_nack:
                continue
            if not msg["acquired"]:
                continue

            msg["acquired"] = False
            delay = msg["failure_base_delay"] * (2 ** msg["failure_count"])
            msg["failure_count"] += 1
            msg["delivery_time"] = time.time() + delay
            print(delay)

    def batch_touch(self, task_ids: Collection[int]) -> None:
        assert self.created
        to_touch = set(task_ids)
        for msg in self.messages:
            if msg["id"] in to_touch:
                continue
            if not msg["acquired"]:
                continue
            msg["acquire_time"] = time.time()

    def list_topics(self) -> List[Tuple[int, int]]:
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

    def get_topic_size(self, topic: int) -> int:
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

    def acquire_topic(
        self, topic_lock_visibility_timeout: int
    ) -> Optional["TopicLock"]:
        assert self.created
        for topic, size in self.list_topics():
            if topic not in self.topic_locks:
                self.topic_locks[topic] = {
                    "acquire_time": time.time(),
                    "visibility_timeout": topic_lock_visibility_timeout,
                }
                return TopicLock(topic, self)
        return None

    def batch_release_topic(self, topics: Collection[int]) -> None:
        assert self.created
        for topic in topics:
            if topic in self.topic_locks:
                del self.topic_locks[topic]

    def batch_touch_topic(self, topics: Collection[int]) -> None:
        assert self.created
        for topic in topics:
            if topic not in self.topic_locks:
                continue
            self.topic_locks[topic]["acquire_time"] = time.time()

    def release_stalled_topic_locks(self) -> int:
        assert self.created
        to_release = [
            topic
            for topic, meta in self.topic_locks.items()
            if meta["acquire_time"] + meta["visibility_timeout"] < time.time()
        ]
        for topic in to_release:
            del self.topic_locks[topic]
        return len(to_release)

    def rate_limit(self, key: bytes, max_events: int, interval_seconds: int) -> bool:
        if len(key) != self.hash_size:
            raise ValueError(
                f"rate limit key size is not HASH_SIZE ({len(key)} != {self.hash_size})"
            )

        if key not in self.rate_limits:
            self.rate_limits[key] = []

        now = time.time()
        self.rate_limits[key] = [t for t in self.rate_limits[key] if t > now]

        if len(self.rate_limits[key]) < max_events:
            self.rate_limits[key].append(now + interval_seconds)
            return True

        return False
