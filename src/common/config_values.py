"""Small helpers for reading typed values from ConfigParser."""

from __future__ import annotations

import configparser


def as_bool(config: configparser.ConfigParser, section: str, key: str, default: bool = False) -> bool:
    if not config.has_option(section, key):
        return default
    return config.get(section, key).strip().lower() in {"1", "true", "yes", "on"}


def as_text(config: configparser.ConfigParser, section: str, key: str, default: str = "") -> str:
    if not config.has_option(section, key):
        return default
    return config.get(section, key).strip()


def as_int(config: configparser.ConfigParser, section: str, key: str, default: int) -> int:
    if not config.has_option(section, key):
        return default
    return int(config.get(section, key).strip())


def as_text_multi(
    config: configparser.ConfigParser,
    section: str,
    keys: list[str],
    default: str = "",
) -> str:
    for key in keys:
        if config.has_option(section, key):
            return config.get(section, key).strip()
    return default
