//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_cteref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

class BoundCTERef : public BoundTableRef {
public:
	BoundCTERef(idx_t bind_index, idx_t cte_index)
	    : BoundTableRef(TableReferenceType::CTE), bind_index(bind_index), cte_index(cte_index) {
	}

	//! The set of columns bound to this base table reference
	vector<string> bound_columns;
	//! The types of the values list
	vector<SQLType> types;
	//! The index in the bind context
	idx_t bind_index;
	//! The index of the cte
	idx_t cte_index;
};
} // namespace duckdb
