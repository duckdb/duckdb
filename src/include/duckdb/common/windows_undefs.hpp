//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/windows_undefs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef WIN32

#ifdef min
#undef min
#endif

#ifdef max
#undef max
#endif

#ifdef ERROR
#undef ERROR
#endif

#ifdef GetCValue
#undef GetCValue
#endif

#ifdef small
#undef small
#endif

#endif
