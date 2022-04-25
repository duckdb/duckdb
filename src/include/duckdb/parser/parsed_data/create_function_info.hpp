//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

struct CreateFunctionInfo : public CreateInfo {
	explicit CreateFunctionInfo(CatalogType type, string schema = DEFAULT_SCHEMA) : CreateInfo(type, schema) {
		D_ASSERT(type == CatalogType::SCALAR_FUNCTION_ENTRY || type == CatalogType::AGGREGATE_FUNCTION_ENTRY ||
		         type == CatalogType::TABLE_FUNCTION_ENTRY || type == CatalogType::PRAGMA_FUNCTION_ENTRY ||
		         type == CatalogType::MACRO_ENTRY || type == CatalogType::TABLE_MACRO_ENTRY);
	}

	//! Function name
	string name;
};

} // namespace duckdb
