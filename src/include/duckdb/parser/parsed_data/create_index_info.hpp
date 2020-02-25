//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/index_type.hpp"

namespace duckdb {

struct CreateIndexInfo : public ParseInfo {
	CreateIndexInfo() : if_not_exists(false) {
	}

	////! Index Type (e.g., B+-tree, Skip-List, ...)
	IndexType idx_type;
	////! Name of the Index
	string index_name;
	////! If it is an unique index
	bool unique = false;

	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;
};

} // namespace duckdb
