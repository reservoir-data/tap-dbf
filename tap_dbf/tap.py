"""Singer SDK classes.

Copyright 2025 Edgar Ramírez-Mondragón.
"""

from __future__ import annotations

import builtins
import logging
import re
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO
from urllib.parse import ParseResult, parse_qsl, urlparse, urlunparse

import fsspec
import singer_sdk.typing as th
from singer_sdk import Stream, Tap
from singer_sdk.singerlib import Catalog, CatalogEntry, MetadataMapping, Schema
from singer_sdk.streams.core import REPLICATION_FULL_TABLE

from tap_dbf.client import FilesystemDBF

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from os import PathLike
    from types import TracebackType

    from dbfread.dbf import DBFField
    from fsspec import AbstractFileSystem
    from singer_sdk.helpers.types import Context

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

    OpenFunc = Callable[[PathLike[bytes], str], BinaryIO]
    RawRecord = dict[str, Any]

logger = logging.getLogger(__name__)


def _dbf_field_to_jsonschema(field: DBFField) -> dict[str, Any]:  # ty: ignore[invalid-type-form]
    """Map a .dbf data type to a JSON schema.

    Args:
        field: The field to map.

    Returns:
        A JSON schema.
    """
    d: dict[str, Any] = {"type": ["null"]}
    if field.type == "N":
        if field.decimal_count == 0:
            d["type"].append("integer")
        else:
            d["type"].append("number")
    elif field.type in {"+", "I"}:
        d["type"].append("integer")
    elif field.type in {"B", "F", "O", "Y"}:
        d["type"].append("number")
    elif field.type == "L":
        d["type"].append("boolean")
    elif field.type == "D":
        d["type"].append("string")
        d["format"] = "date-time"
    elif field.type in {"@", "T"}:
        d["type"].append("string")
        d["format"] = "time"
    else:
        d["type"].append("string")
        d["maxLength"] = field.length

    return d


def normalize_stream_name(filename: str) -> str:
    """Normalize a stream name.

    Args:
        filename: The filename to normalize to a stream name.

    - Replace non-alphanumeric characters with underscores
    - Convert to lowercase
    - Replace multiple consecutive underscores with a single underscore
    - Remove leading and trailing underscores

    Returns:
        The normalized stream name.
    """
    normalized = re.sub(r"[^a-zA-Z0-9]", "_", filename).lower()
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def dbf_table_to_catalog_entry(table: FilesystemDBF) -> CatalogEntry:
    """Convert a DBF table to a catalog entry.

    Args:
        table: The DBF table to convert.

    Returns:
        A catalog entry.
    """
    schema: dict[str, Any] = {"type": "object", "properties": {}}
    primary_keys = []
    for field in table.fields:
        schema["properties"][field.name] = _dbf_field_to_jsonschema(field)
        if field.type == "+":
            primary_keys.append(field.name)
    schema["properties"]["_sdc_filepath"] = {"type": ["string"]}
    schema["properties"]["_sdc_row_index"] = {"type": ["integer"]}
    stream_name = normalize_stream_name(table.name)
    metadata = MetadataMapping.get_standard_metadata(
        schema=schema,
        key_properties=primary_keys,
        replication_method=REPLICATION_FULL_TABLE,
    )
    return CatalogEntry(
        tap_stream_id=stream_name,
        stream=stream_name,
        schema=Schema.from_dict(schema),
        key_properties=primary_keys,
        metadata=metadata,
    )


class PatchOpen:
    """Context helper to patch the builtin open function."""

    def __init__(self: PatchOpen, fs: AbstractFileSystem) -> None:
        """Patch builtins.open with a custom open function.

        Args:
            fs: The filesystem instance to use.
        """
        self.old_impl = _patch_open(fs.open)

    def __enter__(self: Self) -> Self:
        """Create a context for the patched function.

        Returns:
            The PatchOpen context.
        """
        return self

    def __exit__(
        self: PatchOpen,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context and revert patch.

        Args:
            exc_type: The exception type.
            exc_val: The exception value.
            exc_tb: The exception traceback.
        """
        _patch_open(self.old_impl)


def _patch_open(func: OpenFunc) -> OpenFunc:
    """Patch `builtins.open` with `func`.

    Args:
        func: The function to patch `builtins.open` with.

    Returns:
        The original `builtins.open` function.
    """
    old_impl = builtins.open
    builtins.open = func  # type: ignore[assignment]
    return old_impl  # type: ignore[return-value]


class DBFStream(Stream):
    """A dBase file stream."""

    @override
    def __init__(
        self: DBFStream,
        tap: Tap,
        table: FilesystemDBF,
        catalog_entry: CatalogEntry,
    ) -> None:
        """Create a new .DBF file stream.

        Args:
            tap: The tap instance.
            table: The DBF table instance.
            catalog_entry: The catalog entry instance.
        """
        self._table = table
        schema = catalog_entry.schema.to_dict()
        super().__init__(tap, schema=schema, name=catalog_entry.tap_stream_id)

    @override
    def get_records(
        self: DBFStream,
        context: Context | None = None,
    ) -> Iterable[RawRecord]:
        """Get .DBF rows.

        Yields:
            A row of data.
        """
        for index, row in enumerate(self._table):
            row["_sdc_filepath"] = self._table.filename
            row["_sdc_row_index"] = index
            yield row


class TapDBF(Tap):
    """A singer tap for .DBF files."""

    name = "tap-dbf"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "path",
            th.StringType,
            required=True,
            description=(
                "Glob expression where the files are located. Stream names will be "
                "extracted from the file name."
            ),
        ),
        th.Property(
            "fs_root",
            th.StringType,
            default="file://",
            description="The root of the filesystem to read from.",
        ),
        th.Property(
            "ignore_missing_memofile",
            th.BooleanType,
            default=False,
            description=(
                "Whether to proceed reading the file even if the [memofile] is not "
                "present."
            ),
        ),
        th.Property(
            "s3",
            th.ObjectType(
                th.Property(
                    "key",
                    th.StringType,
                    secret=True,
                    description="The AWS key ID.",
                ),
                th.Property(
                    "secret",
                    th.StringType,
                    secret=True,
                    description="The AWS secret key.",
                ),
                th.Property(
                    "endpoint_url",
                    th.StringType,
                    description="The S3 endpoint URL.",
                    examples=[
                        "https://localhost:9000",
                    ],
                ),
            ),
            description="S3 configuration.",
        ),
        th.Property(
            "gcs",
            th.ObjectType(
                th.Property(
                    "token",
                    th.StringType,
                    description="OAuth 2.0 token for GCS.",
                ),
            ),
            description="GCS configuration.",
        ),
    ).to_dict()

    @override
    def __init__(self: TapDBF, *args: Any, **kwargs: Any) -> None:
        """Initialize the tap.

        Args:
            *args: Positional arguments for the Tap initializer.
            **kwargs: Keyword arguments for the Tap initializer.
        """
        self._tables: list[FilesystemDBF] | None = None
        self._url: ParseResult | None = None
        self._fs: AbstractFileSystem | None = None
        super().__init__(*args, **kwargs)

    @property
    def url(self) -> ParseResult:
        """The URL to use."""
        if self._url is None:
            fs_root: str = self.config["fs_root"]
            self._url = urlparse(fs_root)
        return self._url

    @property
    def fs(self) -> AbstractFileSystem:
        """The filesystem to use."""
        if self._fs is None:
            protocol = self.url.scheme
            storage_options = {
                **dict(parse_qsl(self.url.query)),
                **self.config.get(protocol, {}),
            }
            self._fs = fsspec.filesystem(self.url.scheme, **storage_options)
        return self._fs

    @property
    def full_path(self) -> str:
        """The full path to the files."""
        path = self.url.path + self.config["path"]
        if not self.url.hostname:
            hostname, path = path.split("/", 1)
        else:
            hostname = self.url.hostname
        return urlunparse(self.url._replace(query="", netloc=hostname, path=path))

    @property
    def tables(self) -> list[FilesystemDBF]:
        """The tables to discover."""
        if self._tables is None:
            self._tables = [
                FilesystemDBF(
                    filepath,
                    ignorecase=False,
                    ignore_missing_memofile=self.config["ignore_missing_memofile"],
                    filesystem=self.fs,
                )
                for filepath in self.fs.glob(self.full_path)
            ]

        return self._tables

    @cached_property
    def _tables_by_stream_id(self) -> dict[str, FilesystemDBF]:
        """Map stream IDs to tables."""
        return {normalize_stream_name(table.name): table for table in self.tables}

    @override
    @property
    def _singer_catalog(self) -> Catalog:
        """The Singer catalog object."""
        catalog_entries = [dbf_table_to_catalog_entry(table) for table in self.tables]
        return Catalog((entry.tap_stream_id, entry) for entry in catalog_entries)

    @override
    def discover_streams(self: TapDBF) -> list[DBFStream]:
        return [
            DBFStream(
                tap=self,
                table=self._tables_by_stream_id[tap_stream_id],
                catalog_entry=catalog_entry,
            )
            for tap_stream_id, catalog_entry in self.catalog.items()
        ]
