#pragma once

#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct CreateMatViewInfo : public CreateTableInfo {
	CreateMatViewInfo() : CreateTableInfo() {
		this->type = CatalogType::MATVIEW_ENTRY;
	}
	CreateMatViewInfo(string schema, string name) : CreateTableInfo(schema, name) {
		this->type = CatalogType::MATVIEW_ENTRY;
	}
};

} // namespace duckdb
