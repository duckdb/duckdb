//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/warn.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#define DO_PRAGMA(x) _Pragma(#x)
#define NOWARN(warnoption, ...)                                                                                        \
	DO_PRAGMA(GCC diagnostic push)                                                                                     \
	DO_PRAGMA(GCC diagnostic ignored #warnoption)                                                                      \
	__VA_ARGS__                                                                                                        \
	DO_PRAGMA(GCC diagnostic pop)
