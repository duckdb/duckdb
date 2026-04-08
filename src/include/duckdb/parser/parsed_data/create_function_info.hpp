//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
enum class CatalogType : uint8_t;

struct FunctionDescription {
	//! Parameter types (if any)
	vector<LogicalType> parameter_types;
	//! Parameter names (if any)
	vector<string> parameter_names;
	//! The description (if any)
	string description;
	//! Examples (if any)
	vector<string> examples;
	//! Categories (if any)
	vector<string> categories;
};

struct CreateFunctionInfo : public CreateInfo {
	explicit CreateFunctionInfo(CatalogType type, string schema = DEFAULT_SCHEMA);

	//! Function name
	string name;
	//! The function name of which this function is an alias
	string alias_of;
	//! Function description
	vector<FunctionDescription> descriptions;

	DUCKDB_API void CopyFunctionProperties(CreateFunctionInfo &other) const;
};

} // namespace duckdb
