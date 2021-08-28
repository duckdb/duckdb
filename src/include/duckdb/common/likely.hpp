//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/likely.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#if __GNUC__
#define DUCKDB_BUILTIN_EXPECT(cond, expected_value) (__builtin_expect(cond, expected_value))
#else
#define DUCKDB_BUILTIN_EXPECT(cond, expected_value) (cond)
#endif

#define DUCKDB_LIKELY(...)   DUCKDB_BUILTIN_EXPECT((__VA_ARGS__), 1)
#define DUCKDB_UNLIKELY(...) DUCKDB_BUILTIN_EXPECT((__VA_ARGS__), 0)
