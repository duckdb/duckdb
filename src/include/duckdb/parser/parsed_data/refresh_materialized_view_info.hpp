//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/refresh_materialized_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct RefreshMaterializedViewInfo : CreateTableInfo {
	RefreshMaterializedViewInfo() : CreateTableInfo() {
		this->type = CatalogType::MATERIALIZED_VIEW_ENTRY;
	}
	RefreshMaterializedViewInfo(string catalog, string schema, string name);

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
