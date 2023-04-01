//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/warn.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#define DO_PRAGMA(x) _Pragma(#x)

#if defined(__GNUC__)
#define DISABLE_WARN(warnoption)                                                                                       \
	DO_PRAGMA(GCC diagnostic push)                                                                                     \
	DO_PRAGMA(GCC diagnostic ignored #warnoption)
#define RESET_WARN() DO_PRAGMA(GCC diagnostic pop)
#elif defined(__clang__)
#define DISABLE_WARN(warnoption)                                                                                       \
	DO_PRAGMA(clang diagnostic push)                                                                                   \
	DO_PRAGMA(clang diagnostic ignored #warnoption)
#define RESET_WARN() DO_PRAGMA(clang diagnostic pop)
#else
#define DISABLE_WARN(x)
#define RESET_WARN()
#endif
