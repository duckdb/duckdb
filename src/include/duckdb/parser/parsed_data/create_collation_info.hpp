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
	CreateCollationInfo(string name_p, ScalarFunction function_p, bool combinable_p, bool not_required_for_equality_p)
	    : CreateInfo(CatalogType::COLLATION), function(move(function_p)), combinable(combinable_p), not_required_for_equality(not_required_for_equality_p) {
		this->name = move(name_p);
	}

	//! The name of the collation
	string name;
	//! The collation function to push in case collation is required
	ScalarFunction function;
	//! Whether or not the collation can be combined with other collations.
	bool combinable;
	//! Whether or not the collation is required for equality comparisons or not. For many collations a binary comparison for equality comparisons is correct, allowing us to skip the collation in these cases which greatly speeds up processing.
	bool not_required_for_equality;
};

} // namespace duckdb
