"""Entrypoint module, in case you use `python -m tap_dbf`.

Copyright 2025 Edgar Ramírez-Mondragón.
"""

from __future__ import annotations

from tap_dbf.tap import TapDBF

TapDBF.cli()
