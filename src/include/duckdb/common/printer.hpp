//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/printer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Printer is a static class that allows printing to logs or stdout/stderr
class Printer {
public:
	//! Print the object to stderr
	static void Print(string str);
};
} // namespace duckdb
