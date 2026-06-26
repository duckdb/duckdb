//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

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
	explicit CreateFunctionInfo(CatalogType type, Identifier schema = Identifier::DefaultSchema());

	//! The function name of which this function is an alias
	Identifier alias_of;
	//! Function description
	vector<FunctionDescription> descriptions;

	const Identifier &GetFunctionName() const {
		return qualified_name.Name();
	}
	void SetFunctionName(Identifier name) {
		qualified_name.NameMutable() = std::move(name);
	}

	DUCKDB_API void CopyFunctionProperties(CreateFunctionInfo &other) const;
};

} // namespace duckdb
