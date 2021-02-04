//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/assert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#if (defined(DUCKDB_USE_STANDARD_ASSERT) || !defined(DEBUG)) && !defined(DUCKDB_FORCE_ASSERT)

#include <assert.h>
#define D_ASSERT assert
#else
namespace duckdb {
void duckdb_assert_internal(bool condition, const char *condition_name, const char *file, int linenr);
}

#define D_ASSERT(condition) duckdb::duckdb_assert_internal(bool(condition), #condition, __FILE__, __LINE__)

#endif