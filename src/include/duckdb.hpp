//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef __cpp_exceptions
	#error "DuckDB requires C++ Exception support (e.g. -fexceptions in gcc)"
#endif

#include "main/connection.hpp"
#include "main/database.hpp"
#include "main/query_result.hpp"
