#pragma once

#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct CreateMatViewInfo : CreateTableInfo {
	CreateMatViewInfo() : CreateTableInfo() {
		this->type = CatalogType::MATVIEW_ENTRY;
	}
};

} // namespace duckdb
