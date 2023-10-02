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
	static constexpr const TableReferenceType TYPE = TableReferenceType::CTE;

public:
	BoundCTERef(idx_t bind_index, idx_t cte_index, CTEMaterialize materialized_cte)
	    : BoundTableRef(TableReferenceType::CTE), bind_index(bind_index), cte_index(cte_index),
	      materialized_cte(materialized_cte) {
	}

	//! The set of columns bound to this base table reference
	vector<string> bound_columns;
	//! The types of the values list
	vector<LogicalType> types;
	//! The index in the bind context
	idx_t bind_index;
	//! The index of the cte
	idx_t cte_index;
	//! Is this a reference to a materialized CTE?
	CTEMaterialize materialized_cte;
};
} // namespace duckdb
