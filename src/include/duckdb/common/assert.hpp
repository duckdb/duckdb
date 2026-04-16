//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/assert.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/winapi.hpp"

#pragma once

// clang-format off
#if ( \
    /* Not a debug build */ \
    !defined(DEBUG) && \
    /* FORCE_ASSERT is not set (enables assertions even on release mode when set to true) */ \
    !defined(DUCKDB_FORCE_ASSERT) && \
    /* The project is not compiled for Microsoft Visual Studio */ \
    !defined(__MVS__) \
)
// clang-format on

//! On most builds, NDEBUG is defined, turning the assert call into a NO-OP
//! Only the 'else' condition is supposed to check the assertions
#include <assert.h>
#define D_ASSERT assert
namespace duckdb {
DUCKDB_API void DuckDBAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);
}

#else
namespace duckdb {
DUCKDB_API void DuckDBAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);
}

#define D_ASSERT(condition) duckdb::DuckDBAssertInternal(bool(condition), #condition, __FILE__, __LINE__)
#define D_ASSERT_IS_ENABLED

// Runtime toggle for all debug Verify() calls (DataChunk, Vector,
// ColumnBindingResolver, ColumnLifetimeAnalyzer, ...). When false,
// Verify() methods guarded by DUCKDB_DEBUG_VERIFY_GUARD() become
// no-ops. Settable via `SET debug_verification TO false;` so that
// EXPLAIN plans stay readable and tests run faster in debug builds.

#include <atomic>

namespace duckdb {
inline std::atomic<bool> g_debug_verify_enabled = false; // NOLINT: intentionally mutable global
}

// Place at the top of any Verify() method body
// (inside #ifdef D_ASSERT_IS_ENABLED) to respect the global toggle.
#define DUCKDB_DEBUG_VERIFY_GUARD()                                                                                    \
	do {                                                                                                               \
		if (!::duckdb::g_debug_verify_enabled.load(std::memory_order_relaxed)) {                                       \
			return;                                                                                                    \
		}                                                                                                              \
	} while (0)

#endif

//! Force assertion implementation, which always asserts whatever build type is used.
#define ALWAYS_ASSERT(condition) duckdb::DuckDBAssertInternal(bool(condition), #condition, __FILE__, __LINE__)
