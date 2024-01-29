//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/files.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ReadFileFunction {
	static TableFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
