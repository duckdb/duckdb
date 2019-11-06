//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/index_type.hpp"

namespace duckdb {

struct CreateIndexInfo {
	////! Index Type (e.g., B+-tree, Skip-List, ...)
	IndexType index_type;
	////! Name of the Index
	string index_name;
	////! If it is an unique index
	bool unique = false;

	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;

	CreateIndexInfo() : if_not_exists(false) {
	}
};

} // namespace duckdb
