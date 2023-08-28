import time
from typing import Optional, Dict, List, Any, Generator

from squeal.backend.base import Message
from squeal.queue import Queue
from squeal.utils import get_logger, lm

logger = get_logger()


class BufferMessage(Message):
    FIELDS = Message.FIELDS + ["buffer"]

    def __init__(self, *args, buffer: "Buffer", **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = buffer
        self.half_acked = False

    @classmethod
    def from_message(cls, msg: Message, *args, **kwargs) -> "BufferMessage":
        kwargs.update({k: getattr(msg, k) for k in msg.FIELDS})
        return cls(*args, **kwargs)

    def ack(self):
        super().ack()
        if not self.half_acked:
            self.buffer.ack_nack(self.idx)

    def nack(self):
        super().nack()
        if not self.half_acked:
            self.buffer.ack_nack(self.idx)

    def soft_nack(self):
        super().soft_nack()
        if not self.half_acked:
            self.buffer.ack_nack(self.idx)

    def half_ack(self):
        # Remove this from the "processing" count in the buffer, but don't actually ack it in the queue
        # Used when we've finished the part of the processing that we want concurrency limits on (e.g. downloading a
        # webpage) but we have more processing that we want to do before we can say the task is complete.
        if not self.half_acked:
            self.half_acked = True
            self.buffer.ack_nack(self.idx)


class Buffer:
    def __init__(
        self,
        queue: Queue,
        extra_buffer_multiplier: int = 2,
        default_topic_quota: int = 1,
        max_topic_idle: int = 10,
    ):
        self.queue = queue

        self.topic_buffer: Dict[int, List[Message]] = {}
        self.extra_buffer_multiplier = extra_buffer_multiplier
        self.default_topic_quota: int = default_topic_quota
        self.max_topic_idle: int = max_topic_idle

        self.topic_quota: Dict[int, int] = {}
        self.topic_processing: Dict[int, int] = {}
        self.topic_last_get: Dict[int, float] = {}
        self.message_topic: Dict[int, int] = {}

        self.closed = False

    def close(self):
        if self.closed:
            raise RuntimeError
        self.closed = True
        self.queue.release_topics()
        self.queue.nack_all()

    def touch(self) -> None:
        if self.closed:
            raise RuntimeError
        self.queue.touch_all()
        self.queue.touch_topics()

    def _fill_buffer(self, idx: int) -> None:
        held = len(self.topic_buffer[idx])
        quota = self.topic_quota[idx]
        processing = self.topic_processing[idx]
        if held - processing >= quota:
            return
        target = self.extra_buffer_multiplier * quota - held + processing
        msgs = self.queue.batch_get([(idx, target)])
        self.topic_buffer[idx].extend(msgs)

    def get_topic_size(self, idx: int) -> int:
        held = len(self.topic_buffer[idx])
        server = self.queue.get_topic_size(idx)
        return held + server

    def _acquire_topic(self) -> bool:
        topic = self.queue.acquire_topic()
        if topic is None:
            return False
        self.topic_quota[topic.idx] = self.default_topic_quota
        self.topic_processing[topic.idx] = 0
        self.topic_buffer[topic.idx] = []
        self.topic_last_get[topic.idx] = time.time()
        self._fill_buffer(topic.idx)
        return True

    def _drop_topic(self, topic: int):
        if self.topic_processing[topic] > 0:
            raise RuntimeError

        for msg in self.topic_buffer[topic]:
            msg.soft_nack()
        self.queue.release_topic(topic)

        del self.topic_buffer[topic]
        del self.topic_processing[topic]
        del self.topic_quota[topic]
        del self.topic_last_get[topic]

    def _fill_buffers(self) -> None:
        # Check all the checked-out topics and make sure we have messages in the buffer
        # according to the topic's quota
        need_new_topic = True
        for topic_lock in self.queue.list_held_topics():
            topic = topic_lock.idx
            self._fill_buffer(topic)

            allowed = self.topic_quota[topic] - self.topic_processing[topic]
            if self.topic_buffer[topic] and allowed > 0:
                need_new_topic = False

            if self.topic_processing[topic] == 0:
                if not self.topic_buffer[topic]:
                    self._drop_topic(topic)
                elif time.time() - self.topic_last_get[topic] > self.max_topic_idle:
                    self._drop_topic(topic)

        if need_new_topic:
            self._acquire_topic()

    def get(self) -> Optional[BufferMessage]:
        if self.closed:
            raise RuntimeError
        self._fill_buffers()
        out = None
        for topic_lock in self.queue.list_held_topics():
            topic = topic_lock.idx
            q = self.topic_quota[topic]
            p = self.topic_processing[topic]
            b = len(self.topic_buffer[topic])
            allowed = q - p
            if allowed <= 0 or b == 0:
                continue

            msg = self.topic_buffer[topic].pop(-1)
            self.message_topic[msg.idx] = topic
            self.topic_processing[topic] += 1
            out = BufferMessage.from_message(msg, buffer=self)
            self.topic_last_get[topic] = time.time()
            break
        return out

    def ack_nack(self, message_idx: int) -> None:
        if self.closed:
            raise RuntimeError
        topic = self.message_topic.pop(message_idx)
        self.topic_processing[topic] -= 1

    def __iter__(self) -> Generator[BufferMessage, Any, None]:
        while True:
            msg = self.get()
            if msg is None:
                break
            yield msg
