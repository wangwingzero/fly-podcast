from __future__ import annotations

import logging
import sys

from .config import settings

_STDOUT_CONFIGURED = False


def _configure_stdout_encoding() -> None:
    global _STDOUT_CONFIGURED
    if _STDOUT_CONFIGURED:
        return
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8", errors="backslashreplace")
        except Exception:  # noqa: BLE001
            pass
    _STDOUT_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    _configure_stdout_encoding()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
    return logger
