//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_copy_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

struct CreateCopyFunctionInfo : public CreateInfo {
	CreateCopyFunctionInfo(CopyFunction function) : CreateInfo(CatalogType::COPY_FUNCTION), function(function) {
		this->name = function.name;
	}

	//! Function name
	string name;
	//! The table function
	CopyFunction function;
};

} // namespace duckdb
