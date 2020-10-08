//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_views.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

struct DefaultViews {
	static unique_ptr<CreateViewInfo> GetDefaultView(string schema, string name);
};

} // namespace duckdb
