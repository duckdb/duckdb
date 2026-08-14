//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

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
	explicit CreateFunctionInfo(CatalogType type, string schema = DEFAULT_SCHEMA);

	//! Function name
	string name;
	//! The function name of which this function is an alias
	string alias_of;
	//! Function description
	vector<FunctionDescription> descriptions;

	//! NOTE(backport): DuckDB 2.0 hoists these onto `CreateInfo`, which stores a single `QualifiedName` covering the
	//! catalog, schema and name. On this branch `CreateInfo` still keeps `catalog`/`schema` as separate strings and
	//! the name lives on the subclass, so the accessors live here instead. The spelling at the call site is the same
	//! (`info.SetName(x)`), which is what matters for keeping the 2.0 diff small.
	void SetName(string name_p) {
		name = std::move(name_p);
	}
	const string &GetName() const {
		return name;
	}
	const string &GetFunctionName() const {
		return name;
	}
	void SetFunctionName(string name_p) {
		name = std::move(name_p);
	}
	const string &GetEntryName() const override {
		return name;
	}
	void SetEntryName(string name_p) override {
		name = std::move(name_p);
	}

	DUCKDB_API void CopyFunctionProperties(CreateFunctionInfo &other) const;
};

} // namespace duckdb
