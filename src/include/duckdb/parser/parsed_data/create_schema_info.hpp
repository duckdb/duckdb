//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_schema_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

struct CreateSchemaInfo : public CreateInfo {
	CreateSchemaInfo() : CreateInfo(CatalogType::SCHEMA) {
	}
};

} // namespace duckdb
