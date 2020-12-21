//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/show_select_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

struct ShowSelectInfo : public ParseInfo {

	//ShowSelectInfo() : ParseInfo() {
	//}

	//! Return types
	vector<LogicalType> types;
	//! The QueryNode of the view
	unique_ptr<QueryNode> query;
	//! The set of aliases associated with the view
	vector<string> aliases;
};

} // namespace duckdb
