#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct LoggedCSVError {
	idx_t line;
	idx_t column;
	string error;
	string file_name;
	string parsed_value;
};

struct ReadCSVErrorLog {
	vector<LoggedCSVError> errors;
};

struct ReadCSVErrorLogTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

};