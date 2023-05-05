#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct ReadCSVErrorsTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

};