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
#define D_ASSERT_HEAVY D_ASSERT

#elif defined(DUCKDB_USE_STANDARD_ASSERT) && defined(DEBUG)

#include <assert.h>
#define D_ASSERT assert
#define D_ASSERT_HEAVY D_ASSERT

#else

#define D_ASSERT(X)                                                                                                    \
	do {                                                                                                               \
		if (!(X)) {                                                                                                    \
			__builtin_unreachable();                                                                                   \
		}                                                                                                              \
	} while (0)

#define D_ASSERT_HEAVY(X) (void (0))

#endif
