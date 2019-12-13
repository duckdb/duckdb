//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

struct CreateViewInfo : public ParseInfo {
	CreateViewInfo() : schema(DEFAULT_SCHEMA), replace(false) {
	}
	CreateViewInfo(string schema, string view_name) : schema(schema), view_name(view_name), replace(false) {
	}

	//! Schema name to insert to
	string schema;
	//! Table name to insert to
	string view_name;
	//! Aliases of the
	vector<string> aliases;
	//! The QueryNode of the view
	unique_ptr<QueryNode> query;
	//! Replace view if it already exists, instead of failing
	bool replace = false;
};

} // namespace duckdb
