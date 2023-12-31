import time
from collections import Counter
from typing import List, Tuple, Iterable, Optional, Collection
from .base import Backend, Message, TopicLock, PUT_RECORD_COLLECTION


class LocalBackend(Backend):
    """
    This is just a reference implementation for testing purposes.  Don't use it for anything else.  It's deliberately
    unoptimized to make it easy to follow what it's doing.
    """

    def __init__(self):
        self.messages = []
        self.topic_locks = {}
        self.rate_limits = {}
        self.unique_constraint = set()
        self.next_id = 0
        self.created = False

    @property
    def max_payload_size(self) -> Optional[int]:
        return None

    @property
    def hash_size(self) -> int:
        return 16

    def create(self) -> None:
        self.created = True

    def destroy(self) -> None:
        self.created = False

    def batch_put(
        self,
        data: PUT_RECORD_COLLECTION,
        priority: int,
        delay: int,
        failure_base_delay: int,
        rate_limit_seconds: Optional[int] = None,
    ) -> int:
        assert self.created
        self.validate_hashes([x[2] for x in data])
        self.validate_payloads([x[0] for x in data])

        data = self.filter_by_rate_limit(data, rate_limit_seconds)

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
                    "delivery_time": time.time() + delay,
                    "failure_base_delay": failure_base_delay,
                    "failure_count": 0,
                }
            )
            self.next_id += 1

        return len(data) - len(constraint_violations)

    def batch_get(
        self, topic: int, size: int, visibility_timeout: int
    ) -> List["Message"]:
        assert self.created
        now = time.time()
        self.messages.sort(key=lambda x: [-x["priority"], x["id"]])

        output = []
        for msg in self.messages:
            if len(output) >= size:
                break
            if msg["acquired"] and msg["expire_time"] > now:
                continue
            if msg["topic"] != topic:
                continue
            if msg["delivery_time"] > now:
                continue

            msg["acquired"] = True
            msg["expire_time"] = now + visibility_timeout
            output.append(
                Message(
                    msg["payload"],
                    msg["id"],
                    self,
                    visibility_timeout=visibility_timeout,
                )
            )
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

    def batch_soft_nack(self, task_ids: Collection[int]) -> None:
        assert self.created
        to_nack = set(task_ids)
        for idx, msg in enumerate(self.messages):
            if msg["id"] not in to_nack:
                continue
            if not msg["acquired"]:
                continue

            msg["acquired"] = False
            msg["delivery_time"] = time.time()

    def batch_touch(self, task_ids: Collection[int], visibility_timeout: int) -> None:
        assert self.created
        to_touch = set(task_ids)
        for msg in self.messages:
            if msg["id"] in to_touch:
                continue
            if not msg["acquired"]:
                continue
            msg["expire_time"] = time.time() + visibility_timeout

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
            if topic not in self.topic_locks or self.topic_locks[topic] < time.time():
                self.topic_locks[topic] = time.time() + topic_lock_visibility_timeout
                return TopicLock(topic, self, topic_lock_visibility_timeout)
        return None

    def batch_release_topic(self, topics: Collection[int]) -> None:
        assert self.created
        for topic in topics:
            if topic in self.topic_locks:
                del self.topic_locks[topic]

    def batch_touch_topic(
        self, topics: Collection[int], topic_lock_visibility_timeout: int
    ) -> None:
        assert self.created
        for topic in topics:
            if topic not in self.topic_locks:
                continue
            self.topic_locks[topic] = time.time() + topic_lock_visibility_timeout

    def rate_limit(self, hshes: Iterable[bytes], interval_seconds: int) -> List[bytes]:
        self.validate_hashes(hshes)

        now = time.time()
        out = []
        for hsh in hshes:
            if hsh not in self.rate_limits or self.rate_limits[hsh] <= now:
                self.rate_limits[hsh] = now + interval_seconds
                out.append(hsh)
        return out

    def override_rate_limit(
        self, hshes: Collection[bytes], interval_seconds: int
    ) -> None:
        self.validate_hashes(hshes)

        if interval_seconds > 0:
            for hsh in hshes:
                self.rate_limits[hsh] = time.time() + interval_seconds
        else:
            for hsh in hshes:
                self.rate_limits.pop(hsh, 0)
