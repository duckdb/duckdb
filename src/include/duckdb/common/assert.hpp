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
[[noreturn]] DUCKDB_API void DuckDBAssertInternal(const char *condition_name, const char *file, int linenr);
}

#else
namespace duckdb {
[[noreturn]] DUCKDB_API void DuckDBAssertInternal(const char *condition_name, const char *file, int linenr);
}

// Check the condition at the call site; only invoke the out-of-line helper
// when it fails. This keeps the successful path free of argument setup and
// function-call overhead -- the overwhelmingly common case.
#define D_ASSERT(condition)                                                                                            \
	do {                                                                                                               \
		if (!(condition)) [[unlikely]] {                                                                               \
			duckdb::DuckDBAssertInternal(#condition, __FILE__, __LINE__);                                              \
		}                                                                                                              \
	} while (false)
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
		if (!::duckdb::g_debug_verify_enabled.load(std::memory_order_relaxed)) [[likely]] {                            \
			return;                                                                                                    \
		}                                                                                                              \
	} while (false)

#endif

//! Force assertion implementation, which always asserts whatever build type is used.
#define ALWAYS_ASSERT(condition)                                                                                       \
	do {                                                                                                               \
		if (!(condition)) [[unlikely]] {                                                                               \
			duckdb::DuckDBAssertInternal(#condition, __FILE__, __LINE__);                                              \
		}                                                                                                              \
	} while (false)
