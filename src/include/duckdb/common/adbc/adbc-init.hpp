// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#ifndef DUCKDB_ADBC_INIT
#define DUCKDB_ADBC_INIT

#include "duckdb/common/adbc/adbc.hpp"

#ifdef __cplusplus
extern "C" {
#endif

//! We gotta leak the symbols of the init function
duckdb_adbc::AdbcStatusCode duckdb_adbc_init(size_t count, struct duckdb_adbc::AdbcDriver *driver,
                                             struct duckdb_adbc::AdbcError *error);

#ifdef __cplusplus
}
#endif

#endif
