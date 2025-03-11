//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/windows_undefs.hpp
//
//
//===----------------------------------------------------------------------===//

// Do not add a header inclusion guard to this file. Otherwise these Win32 macros
// may get defined and stomp on DuckDB symbols

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

#ifdef small
#undef small
#endif

#ifdef CreateDirectory
#undef CreateDirectory
#endif

#ifdef MoveFile
#undef MoveFile
#endif

#ifdef RemoveDirectory
#undef RemoveDirectory
#endif

#ifdef UUID
#undef UUID
#endif

#endif
