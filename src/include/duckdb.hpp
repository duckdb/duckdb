//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#if !defined(__cpp_exceptions)
	#error "DuckDB requires C++ Exception support(-fexceptions in gcc)"
#endif

#include "main/connection.hpp"
#include "main/database.hpp"
#include "main/query_result.hpp"
