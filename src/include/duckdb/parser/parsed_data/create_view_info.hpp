//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

struct CreateViewInfo {
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

	CreateViewInfo() : schema(DEFAULT_SCHEMA), replace(false) {
	}
	CreateViewInfo(string schema, string view_name) : schema(schema), view_name(view_name), replace(false) {
	}
};

} // namespace duckdb
