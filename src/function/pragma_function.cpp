#include "duckdb/function/pragma_function.hpp"

namespace duckdb {

PragmaFunction PragmaFunction::PragmaCall(string name, pragma_query_t query, vector<LogicalType> arguments, LogicalType varargs = LogicalType::INVALID) {
	return PragmaFunction(name, PragmaType::CALL, query, nullptr, move(arguments), move(varargs));
}

PragmaFunction PragmaFunction::PragmaCall(string name, pragma_function_t function, vector<LogicalType> arguments, LogicalType varargs = LogicalType::INVALID) {
	return PragmaFunction(name, PragmaType::CALL, nullptr, function, move(arguments), move(varargs));
}

PragmaFunction PragmaFunction::PragmaStatement(string name, pragma_query_t query) {
	vector<LogicalType> types;
	return PragmaFunction(name, PragmaType::NOTHING, query, nullptr, types, LogicalType::INVALID);
}

PragmaFunction PragmaFunction::PragmaStatement(string name, pragma_function_t function) {
	vector<LogicalType> types;
	return PragmaFunction(name, PragmaType::NOTHING, nullptr, function, types, LogicalType::INVALID);
}

PragmaFunction PragmaFunction::PragmaAssignment(string name, LogicalType type, pragma_query_t query) {
	vector<LogicalType> types { move(type) };
	return PragmaFunction(name, PragmaType::ASSIGNMENT, query, nullptr, types, LogicalType::INVALID);
}

PragmaFunction PragmaFunction::PragmaAssignment(string name, LogicalType type, pragma_function_t function) {
	vector<LogicalType> types { move(type) };
	return PragmaFunction(name, PragmaType::ASSIGNMENT, nullptr, function, types, LogicalType::INVALID);
}

}