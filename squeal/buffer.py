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

    @classmethod
    def from_message(cls, msg: Message, *args, **kwargs) -> "BufferMessage":
        kwargs.update({k: getattr(msg, k) for k in msg.FIELDS})
        return cls(*args, **kwargs)

    def ack(self):
        super().ack()
        self.buffer.ack(self.idx)

    def nack(self):
        super().nack()
        self.buffer.nack(self.idx)


class Buffer:
    def __init__(
        self,
        queue: Queue,
        extra_buffer_multiplier: int = 2,
        default_topic_quota: int = 1,
    ):
        self.queue = queue

        self.topic_buffer: Dict[int, List[Message]] = {}
        self.extra_buffer_multiplier = extra_buffer_multiplier
        self.default_topic_quota: int = default_topic_quota
        self.topic_quota: Dict[int, int] = {}
        self.topic_processing: Dict[int, int] = {}
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
            logger.debug(
                lm(
                    "Buffer._fill_buffer()",
                    {
                        "action": "none",
                        "idx": idx,
                        "n_held": held,
                        "n_quota": quota,
                        "n_processing": processing,
                    },
                )
            )
            return
        target = self.extra_buffer_multiplier * quota - held + processing
        msgs = self.queue.batch_get([(idx, target)])
        self.topic_buffer[idx].extend(msgs)
        logger.debug(
            lm(
                "Buffer._fill_buffer()",
                {
                    "action": "get",
                    "idx": idx,
                    "n_held": held,
                    "n_quota": quota,
                    "n_processing": processing,
                    "target": target,
                    "new_msgs": len(msgs),
                },
            )
        )

    def _acquire_topic(self) -> bool:
        topic = self.queue.acquire_topic()
        if topic is None:
            logger.debug(lm("Buffer._acquire_topic()", {"result": "failure"}))
            return False
        self.topic_quota[topic.idx] = self.default_topic_quota
        self.topic_processing[topic.idx] = 0
        self.topic_buffer[topic.idx] = []
        logger.debug(
            lm(
                "Buffer._acquire_topic()",
                {
                    "result": "success",
                    "topic_id": topic.idx,
                },
            )
        )
        self._fill_buffer(topic.idx)
        return True

    def _drop_topic(self, topic: int):
        if self.topic_processing[topic] > 0:
            raise RuntimeError

        logger.debug(
            lm(
                "Buffer._drop_topic()",
                {"topic_id": topic, "buffer_size": len(self.topic_buffer[topic])},
            )
        )

        for msg in self.topic_buffer[topic]:
            msg.nack()
        self.queue.release_topic(topic)

        del self.topic_buffer[topic]
        del self.topic_processing[topic]
        del self.topic_quota[topic]

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

            if self.topic_processing[topic] == 0 and not self.topic_buffer[topic]:
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
            logger.debug(
                lm(
                    "Buffer.get() topic",
                    {
                        "topic_id": topic,
                        "n_held": b,
                        "n_quota": q,
                        "n_processing": p,
                    },
                )
            )
            if allowed <= 0 or b == 0:
                continue

            msg = self.topic_buffer[topic].pop(-1)
            self.message_topic[msg.idx] = topic
            self.topic_processing[topic] += 1
            out = BufferMessage.from_message(msg, buffer=self)
            break
        logger.debug(
            lm(
                "Buffer.get() result",
                {
                    "success": ("true" if out is not None else "false"),
                },
            )
        )
        return out

    def ack(self, message_idx: int) -> None:
        if self.closed:
            raise RuntimeError
        topic = self.message_topic[message_idx]
        self.topic_processing[topic] -= 1

        logger.debug(
            lm(
                "Buffer.ack()",
                {
                    "topic_id": topic,
                    "n_processing": self.topic_processing[topic],
                },
            )
        )

    def nack(self, message_idx: int) -> None:
        if self.closed:
            raise RuntimeError
        topic = self.message_topic[message_idx]
        self.topic_processing[topic] -= 1

        logger.debug(
            lm(
                "Buffer.nack()",
                {
                    "topic_id": topic,
                    "n_processing": self.topic_processing[topic],
                },
            )
        )

    def __iter__(self) -> Generator[BufferMessage, Any, None]:
        while True:
            msg = self.get()
            if msg is None:
                break
            yield msg
