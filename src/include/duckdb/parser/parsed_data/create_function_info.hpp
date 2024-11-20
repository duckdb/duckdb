//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

struct CreateFunctionInfo : public CreateInfo {
	explicit CreateFunctionInfo(CatalogType type, string schema = DEFAULT_SCHEMA);

	//! Function name
	string name;
	//! The description (if any)
	string description;
	//! Parameter names (if any)
	vector<string> parameter_names;
	//! The example (if any)
	string example;

	DUCKDB_API void CopyFunctionProperties(CreateFunctionInfo &other) const;
};

} // namespace duckdb
