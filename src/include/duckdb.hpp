//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#if !defined(__cpp_exceptions) || !defined(__cpp_rtti)
	#error "DuckDB requires C++ Exceptions and Run-Time Type Information (-fexceptions -frtti in gcc)"
#endif

#include "main/connection.hpp"
#include "main/database.hpp"
#include "main/query_result.hpp"
