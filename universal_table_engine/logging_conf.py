from __future__ import annotations

import logging
import sys
from typing import Any, Dict

import structlog


def configure_logging(level: str = "INFO") -> None:
    timestamper = structlog.processors.TimeStamper(fmt="iso")

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            timestamper,
            _rename_event_key,
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper(), logging.INFO)),
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level.upper())


def _rename_event_key(logger: Any, name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    event = event_dict.pop("event", None)
    if event is not None:
        event_dict["message"] = event
    return event_dict


__all__ = ["configure_logging"]
