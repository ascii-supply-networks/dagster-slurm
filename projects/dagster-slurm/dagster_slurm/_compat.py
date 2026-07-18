"""Compatibility helpers for supported Python versions."""

try:
    import tomllib as _tomllib
except ImportError:
    import tomli as _tomllib

tomllib = _tomllib
