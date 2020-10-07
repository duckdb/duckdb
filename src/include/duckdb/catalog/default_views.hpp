//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default_views.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

struct DefaultViews {
	static unique_ptr<CreateViewInfo> GetDefaultView(string name);
};

} // namespace duckdb
