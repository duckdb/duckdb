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
	//! Types of projected columns
	vector<LogicalType> types;
	//! The QueryNode of select query
	unique_ptr<QueryNode> query;
	//! Aliases of projected columns
	vector<string> aliases;

	unique_ptr<ShowSelectInfo> Copy() {
		auto result = make_unique<ShowSelectInfo>();
		result->types = types;
		result->query = query->Copy();
		result->aliases = aliases;
		return result;
	}
};

} // namespace duckdb
