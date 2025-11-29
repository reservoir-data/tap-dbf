"""Test tap-dbf.

Copyright 2025 Edgar Ramírez-Mondragón.
"""

from __future__ import annotations

from singer_sdk.testing import get_tap_test_class

from tap_dbf.tap import TapDBF

TestTapDBF = get_tap_test_class(
    TapDBF,
    config={
        "path": "/files/*.dbf",
        "fs_root": "file://tests/data",
        "ignore_missing_memofile": True,
    },
)

TestTapDBFNoRoot = get_tap_test_class(
    TapDBF,
    config={
        "path": "tests/data/files/*.dbf",
        "ignore_missing_memofile": True,
    },
)
