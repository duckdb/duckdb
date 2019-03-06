//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/printer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

class Deserializer;
class Serializer;

//! Printer is a static class that allows printing to logs or stdout/stderr
class Printer {
public:
	//! Print the object to stderr
	static void Print(string str);
};
} // namespace duckdb
