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
	CreateFunctionInfo(CatalogType type) : CreateInfo(type) {
		assert(type == CatalogType::SCALAR_FUNCTION_ENTRY || type == CatalogType::AGGREGATE_FUNCTION_ENTRY ||
		       type == CatalogType::TABLE_FUNCTION_ENTRY || type == CatalogType::PRAGMA_FUNCTION_ENTRY);
	}

	//! Function name
	string name;
};

} // namespace duckdb
