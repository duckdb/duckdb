//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_schemas.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

struct DefaultSchemas {
	static bool GetDefaultSchema(string schema);
};

} // namespace duckdb
