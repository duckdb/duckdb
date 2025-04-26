//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/refresh_matview_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct RefreshMatViewInfo : CreateTableInfo {
	RefreshMatViewInfo() : CreateTableInfo() {
		this->type = CatalogType::MATVIEW_ENTRY;
	}
	RefreshMatViewInfo(string catalog, string schema, string name);

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
