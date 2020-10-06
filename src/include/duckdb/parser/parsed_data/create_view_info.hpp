//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

struct CreateViewInfo : public CreateInfo {
	CreateViewInfo() : CreateInfo(CatalogType::VIEW_ENTRY) {
	}
	CreateViewInfo(string schema, string view_name)
	    : CreateInfo(CatalogType::VIEW_ENTRY, schema), view_name(view_name) {
	}

	//! Table name to insert to
	string view_name;
	//! The SQL query (if any)
	string sql;
	//! Aliases of the view
	vector<string> aliases;
	//! Return types
	vector<LogicalType> types;
	//! The SelectStatement of the view
	unique_ptr<SelectStatement> query;
};

} // namespace duckdb
