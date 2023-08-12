from typing import Tuple, List, Dict, Iterable, Optional
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
        auto_create: bool = True,
    ):
        self.backend = backend
        self.new_message_delay = new_message_delay
        self.failure_base_delay = failure_base_delay
        self.visibility_timeout = visibility_timeout
        self.topic_lock_visibility_timeout = topic_lock_visibility_timeout

        self.messages: Dict[int, "Message"] = {}
        self.held_topics: Dict[int, "TopicLock"] = {}

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
        self, item: bytes, topic: int, hsh: Optional[bytes] = None, priority: int = 0
    ) -> None:
        return self.batch_put(items=[(item, topic, hsh)], priority=priority)

    def batch_put(
        self, items: Iterable[Tuple[bytes, int, Optional[bytes]]], priority: int = 0
    ) -> None:
        self.backend.batch_put(
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
        for topic, size in topics:
            if size <= 0:
                continue
            topic_msgs = self.backend.batch_get(topic, size)
            if len(topic_msgs) < size:
                # XXX consider moving this behavior into the Backend
                if self.backend.release_stalled_messages(topic) > 0:
                    topic_msgs.extend(
                        self.backend.batch_get(topic, size - len(topic_msgs))
                    )
            out.extend(topic_msgs)
        for msg in out:
            self.messages[msg.idx] = msg
        return out

    def _prune_stored_messages(self):
        # XXX
        released = [msg.idx for msg in self.messages.values() if msg.released]
        for idx in released:
            del self.messages[idx]

    def touch_all(self) -> None:
        # XXX
        self._prune_stored_messages()
        self.backend.batch_touch(self.messages.keys())

    def nack_all(self) -> None:
        # XXX
        self._prune_stored_messages()
        self.backend.batch_nack(self.messages.keys())
        self.messages.clear()

    def list_topics(self) -> List[Tuple[int, int]]:
        return self.backend.list_topics()

    def get_topic_size(self, topic: int) -> int:
        return self.backend.get_topic_size(topic)

    def acquire_topic(self) -> "TopicLock":
        topic = self.backend.acquire_topic(
            topic_lock_visibility_timeout=self.topic_lock_visibility_timeout
        )
        self.held_topics[topic.idx] = topic
        return topic

    def release_topics(self) -> None:
        self.backend.batch_release_topic(self.held_topics.keys())
        self.held_topics = {}

    def touch_topics(self) -> None:
        self.backend.batch_touch_topic(self.held_topics.keys())


__all__ = ["Queue"]
