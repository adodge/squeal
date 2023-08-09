import abc
import time
from typing import Tuple, Type, Union, List, Optional


DEFAULT_TIMEOUT = -1
DEFAULT_POLL_INTERVAL = 1
PAYLOAD_MAX_SIZE = 1024


class QueueEmpty(BaseException):
    pass


class Backend(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    def create(self) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        raise NotImplementedError

    def put(self, item: bytes, topic: int) -> None:
        raise NotImplementedError

    def release_stalled_tasks(self, topic: int) -> int:
        raise NotImplementedError

    def get(self, topic: int) -> "Message":
        raise NotImplementedError

    def ack(self, task_id: int) -> None:
        raise NotImplementedError

    def nack(self, task_id: int) -> None:
        raise NotImplementedError

    def topics(self) -> List[Tuple[int, int]]:
        raise NotImplementedError

    def size(self, topic: int) -> int:
        raise NotImplementedError


class Message:
    def __init__(self, payload: bytes, idx: int, backend: Backend):
        self.payload = payload
        self.idx = idx
        self.backend = backend
        self.status = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.status is None:
            self.nack()

    def ack(self):
        if self.status is not None:
            raise RuntimeError(
                "Trying to ack a Message that's already been acked or nacked"
            )
        self.status = True
        self.backend.ack(self.idx)

    def nack(self):
        if self.status is not None:
            raise RuntimeError(
                "Trying to nack a Message that's already been acked or nacked"
            )
        self.status = False
        self.backend.nack(self.idx)


class Queue:
    """
    FIFO(-ish) queue backed by a SQL table
    """

    def __init__(
        self,
        backend_instance_or_class: Union[Backend, Type[Backend]],
        *backend_args,
        **backend_kwargs,
    ):
        self.default_timeout: int = backend_kwargs.pop(
            "default_timeout", DEFAULT_TIMEOUT
        )
        self.default_poll_interval: int = backend_kwargs.pop(
            "default_poll_interval", DEFAULT_POLL_INTERVAL
        )

        if self.default_poll_interval <= 0:
            raise RuntimeError("Poll interval must be positive")

        if isinstance(backend_instance_or_class, Backend):
            self.backend = backend_instance_or_class
            if backend_args or backend_kwargs:
                raise RuntimeError(
                    "If you provide a Backend instance, you cannot also provide arguments"
                )
        elif issubclass(backend_instance_or_class, Backend):
            self.backend = backend_instance_or_class(*backend_args, **backend_kwargs)
        else:
            raise RuntimeError(
                "Must provide either am Backend instance or a class and arguments"
            )

    def create(self) -> None:
        self.backend.create()

    def destroy(self) -> None:
        self.backend.destroy()

    def put(self, item: bytes, topic: int) -> None:
        if len(item) > PAYLOAD_MAX_SIZE:
            raise RuntimeError("Payload is larger than PAYLOAD_MAX_SIZE")
        self.backend.put(item, topic)

    def get_nowait(self, topic: int) -> "Message":
        try:
            return self.backend.get(topic)
        except QueueEmpty:
            if self.backend.release_stalled_tasks(topic) == 0:
                raise QueueEmpty()
        return self.backend.get(topic)

    def get(
        self,
        topic: int,
        timeout: Optional[int] = None,
        poll_interval: Optional[int] = None,
    ) -> "Message":
        if timeout is None:
            timeout = self.default_timeout
        if poll_interval is None:
            poll_interval = self.default_poll_interval

        if timeout == 0:
            return self.get_nowait(topic)

        t0 = time.time()
        while True:
            t1 = time.time()
            if timeout > 0 and timeout <= t1 - t0:
                raise QueueEmpty()
            try:
                return self.get_nowait(topic)
            except QueueEmpty:
                pass
            time.sleep(poll_interval)

    def topics(self) -> List[Tuple[int, int]]:
        return self.backend.topics()

    def size(self, topic: int) -> int:
        return self.backend.size(topic)


class MonoQueue(Queue):
    """
    FIFO(-ish) queue backed by a SQL table, with only one topic
    """

    def __init__(
        self,
        backend_instance_or_class: Union[Backend, Type[Backend]],
        *backend_args,
        **backend_kwargs,
    ):
        self.topic = backend_kwargs.pop("topic", 0)
        super().__init__(backend_instance_or_class, *backend_args, **backend_kwargs)

    def put(self, item: bytes, topic=None) -> None:
        if topic is not None:
            raise RuntimeError("Trying to use a MonoQueue with an explicit topic.")
        super().put(item, self.topic)

    def get_nowait(self, topic=None) -> "Message":
        if topic is not None:
            raise RuntimeError("Trying to use a MonoQueue with an explicit topic.")
        return super().get_nowait(self.topic)

    def get(
        self,
        topic=None,
        timeout: Optional[int] = None,
        poll_interval: Optional[int] = None,
    ) -> "Message":
        if topic is not None:
            raise RuntimeError("Trying to use a MonoQueue with an explicit topic.")
        return super().get(
            topic=self.topic, timeout=timeout, poll_interval=poll_interval
        )

    def topics(self) -> List[Tuple[int, int]]:
        raise RuntimeError("Trying to query topics on MonoQueue")

    def size(self, topic=None) -> int:
        if topic is not None:
            raise RuntimeError("Trying to use a MonoQueue with an explicit topic.")
        return super().size(self.topic)


__all__ = ["Queue", "MonoQueue", "Message", "QueueEmpty", "Backend", "PAYLOAD_MAX_SIZE"]
