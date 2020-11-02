//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/show_select_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

//#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

struct ShowSelectInfo : public ParseInfo {

	ShowSelectInfo(string schema) : ParseInfo() {
	}

	//! Return types
	vector<SQLType> types;
	//! The QueryNode of the view
	unique_ptr<QueryNode> query;
	//! The set of aliases associated with the view
	vector<string> aliases;
};

} // namespace duckdb
