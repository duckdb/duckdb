# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Low-level ADBC bindings for the DuckDB driver."""

import enum
import functools
import typing

import adbc_driver_manager

from ._version import __version__  # noqa:F401

__all__ = ["StatementOptions", "connect"]


class StatementOptions(enum.Enum):
    """Statement options specific to the DuckDB driver."""

    #: The number of rows per batch. Defaults to 1024.
    BATCH_ROWS = "adbc.duckdb.query.batch_rows"


def connect(uri: typing.Optional[str] = None) -> adbc_driver_manager.AdbcDatabase:
    """Create a low level ADBC connection to DuckDB."""
    if uri is None:
        return adbc_driver_manager.AdbcDatabase(driver=_driver_path())
    return adbc_driver_manager.AdbcDatabase(driver=_driver_path(), uri=uri)


@functools.cache
def _driver_path() -> str:
    import importlib.resources
    import pathlib
    import sys

    driver = "duckdb"

    # Wheels bundle the shared library
    root = importlib.resources.files(__package__)
    # The filename is always the same regardless of platform
    entrypoint = root.joinpath(f"lib{driver}.dylib")
    if entrypoint.is_file():
        return str(entrypoint)

    # Search sys.prefix + '/lib' (Unix, Conda on Unix)
    root = pathlib.Path(sys.prefix)
    for filename in (f"lib{driver}.so", f"lib{driver}.dylib"):
        entrypoint = root.joinpath("lib", filename)
        if entrypoint.is_file():
            return str(entrypoint)

    # Conda on Windows
    entrypoint = root.joinpath("bin", f"{driver}.dll")
    if entrypoint.is_file():
        return str(entrypoint)

    # Let the driver manager fall back to (DY)LD_LIBRARY_PATH/PATH
    # (It will insert 'lib', 'so', etc. as needed)
    return driver