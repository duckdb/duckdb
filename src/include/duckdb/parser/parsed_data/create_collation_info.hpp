//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_collation_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

struct CreateCollationInfo : public CreateInfo {
	CreateCollationInfo(string name_p, ScalarFunction function_p, bool combinable_p)
	    : CreateInfo(CatalogType::COLLATION), function(move(function_p)), combinable(combinable_p) {
		this->name = move(name_p);
	}

	string name;
	ScalarFunction function;
	bool combinable;
};

} // namespace duckdb
