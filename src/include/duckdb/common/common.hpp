//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef _WIN32
#ifdef DUCKDB_MAIN_LIBRARY
#include "duckdb/common/windows.hpp"
#endif
#endif

#include "duckdb/common/constants.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
