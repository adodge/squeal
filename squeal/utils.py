import dataclasses
import json
import logging
from typing import Dict, Any, Union

LOGGING_MODE = "normal"


def enable_structured_logging():
    global LOGGING_MODE
    LOGGING_MODE = "structured"


def set_log_level(level: Union[int, str]):
    get_logger().setLevel(level)


def get_logger():
    return logging.getLogger("squeal")


@dataclasses.dataclass
class LogMessage:
    message: str
    fields: Dict[str, Any]

    def __str__(self) -> str:
        if LOGGING_MODE == "normal":
            return f"{self.message} >>> {json.dumps(self.fields)}"
        if LOGGING_MODE == "structured":
            return json.dumps({"message": self.message, **self.fields})


lm = LogMessage
