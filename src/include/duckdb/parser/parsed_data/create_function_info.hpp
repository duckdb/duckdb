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
		assert(type == CatalogType::SCALAR_FUNCTION || type == CatalogType::AGGREGATE_FUNCTION);
	}

	//! Function name
	string name;
};

} // namespace duckdb
