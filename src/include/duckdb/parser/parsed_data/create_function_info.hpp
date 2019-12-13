//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

enum class FunctionType : uint8_t { SCALAR = 0, AGGREGATE = 1 };

struct CreateFunctionInfo : public ParseInfo {
	CreateFunctionInfo(FunctionType type) : type(type), schema(DEFAULT_SCHEMA), or_replace(false) {
	}

	//! The type of function (scalar or aggregate)
	FunctionType type;
	//! Schema name
	string schema;
	//! Function name
	string name;
	//! Replace function if it already exists instead of failing
	bool or_replace;
};

} // namespace duckdb
