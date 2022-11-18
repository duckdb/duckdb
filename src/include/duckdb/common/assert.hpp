//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/assert.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/winapi.hpp"

#pragma once

#if defined(DUCKDB_FORCE_ASSERT) || (!defined(DUCKDB_USE_STANDARD_ASSERT) && defined(DEBUG))

namespace duckdb {
DUCKDB_API void DuckDBAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);
}

#define D_ASSERT(condition) duckdb::DuckDBAssertInternal(bool(condition), #condition, __FILE__, __LINE__)

#elif defined(DUCKDB_USE_STANDARD_ASSERT) && defined(DEBUG)

#include <assert.h>
#define D_ASSERT assert

#else

#define D_ASSERT(X) do { if (!(X)) { __builtin_unreachable(); } } while (0)

#endif
