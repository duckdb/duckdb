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

__all__ = ["StatementOptions", "connect"]


class StatementOptions(enum.Enum):
    """Statement options specific to the DuckDB driver."""

    #: The number of rows per batch. Defaults to 2048.
    BATCH_ROWS = "adbc.duckdb.query.batch_rows"


def connect(path: typing.Optional[str] = None) -> adbc_driver_manager.AdbcDatabase:
    """Create a low level ADBC connection to DuckDB."""
    if path is None:
        return adbc_driver_manager.AdbcDatabase(driver=_driver_path(), entrypoint="duckdb_adbc_init")
    return adbc_driver_manager.AdbcDatabase(driver=_driver_path(), entrypoint="duckdb_adbc_init", path=path)


@functools.cache
def _driver_path() -> str:
    import duckdb

    return duckdb.duckdb.__file__
