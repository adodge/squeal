import math
from typing import Tuple, List, Dict, Iterable, Optional, Collection
from squeal.backend.base import Backend, Message, TopicLock


class Queue:
    """
    FIFO(-ish) queue backed by a SQL table
    """

    def __init__(
        self,
        backend: Backend,
        new_message_delay: int = 0,
        failure_base_delay: int = 1,
        visibility_timeout: int = 60,
        topic_lock_visibility_timeout: int = 60 * 15,
        rate_limit_per_hour: Optional[float] = None,
        auto_create: bool = True,
    ):
        self.backend = backend
        self.new_message_delay = new_message_delay
        self.failure_base_delay = failure_base_delay
        self.visibility_timeout = visibility_timeout
        self.topic_lock_visibility_timeout = topic_lock_visibility_timeout
        self.rate_limit_per_hour = rate_limit_per_hour

        self._held_messages: Dict[int, "Message"] = {}
        self._held_topics: Dict[int, "TopicLock"] = {}

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

    @property
    def _rate_limit_interval_seconds(self) -> Optional[int]:
        if self.rate_limit_per_hour is None:
            return None
        return math.ceil(60 * 60 / self.rate_limit_per_hour)

    def put(
        self, item: bytes, topic: int, hsh: Optional[bytes] = None, priority: int = 0
    ) -> int:
        return self.batch_put(items=[(item, topic, hsh)], priority=priority)

    def batch_put(
        self, items: Collection[Tuple[bytes, int, Optional[bytes]]], priority: int = 0
    ) -> int:
        interval = self._rate_limit_interval_seconds
        if interval is not None:
            approved_items = []
            for item, topic, hsh in items:
                if hsh is None or self.backend.rate_limit(
                    hsh, max_events=1, interval_seconds=interval
                ):
                    approved_items.append((item, topic, hsh))
            items = approved_items

        return self.backend.batch_put(
            items,
            priority,
            self.new_message_delay,
            self.failure_base_delay,
            self.visibility_timeout,
        )

    def get(self, topic: int) -> Optional["Message"]:
        msgs = self.batch_get([(topic, 1)])
        return msgs[0] if msgs else None

    def batch_get(self, topics: Iterable[Tuple[int, int]]) -> List["Message"]:
        out = []
        self.backend.release_stalled_messages()
        for topic, size in topics:
            if size <= 0:
                continue
            topic_msgs = self.backend.batch_get(topic, size)
            out.extend(topic_msgs)
        for msg in out:
            self._held_messages[msg.idx] = msg
        return out

    def _prune_held_messages(self):
        # XXX
        released = [msg.idx for msg in self._held_messages.values() if msg.released]
        for idx in released:
            del self._held_messages[idx]

    def touch_all(self) -> None:
        # XXX
        self._prune_held_messages()
        self.backend.batch_touch(self._held_messages.keys())

    def nack_all(self) -> None:
        # XXX
        self._prune_held_messages()
        self.backend.batch_nack(self._held_messages.keys())
        self._held_messages.clear()

    def list_topics(self) -> List[Tuple[int, int]]:
        return self.backend.list_topics()

    def get_topic_size(self, topic: int) -> int:
        return self.backend.get_topic_size(topic)

    def _prune_held_topics(self):
        # XXX
        released = [i for i, t in self._held_topics.items() if t.released]
        for i in released:
            del self._held_topics[i]

    def acquire_topic(self) -> Optional["TopicLock"]:
        self.backend.release_stalled_topic_locks()
        topic = self.backend.acquire_topic(
            topic_lock_visibility_timeout=self.topic_lock_visibility_timeout
        )
        if topic is None:
            return None
        self._held_topics[topic.idx] = topic
        return topic

    def release_topics(self) -> None:
        self._prune_held_topics()
        self.backend.batch_release_topic(self._held_topics.keys())
        self._held_topics = {}

    def release_topic(self, topic: int) -> None:
        self._prune_held_topics()
        if topic not in self._held_topics:
            return
        self._held_topics[topic].release()
        self._prune_held_topics()

    def touch_topics(self) -> None:
        self._prune_held_topics()
        self.backend.batch_touch_topic(self._held_topics.keys())

    def list_held_topics(self) -> List["TopicLock"]:
        self._prune_held_topics()
        return list(self._held_topics.values())


__all__ = ["Queue"]
