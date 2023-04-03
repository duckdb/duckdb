//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_pivotref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"

namespace duckdb {

//! Represents a join
class BoundPivotRef : public BoundTableRef {
public:
	explicit BoundPivotRef()
	    : BoundTableRef(TableReferenceType::PIVOT) {
	}

	idx_t bind_index;
	//! The binder used to bind the child of the pivot
	shared_ptr<Binder> child_binder;
	//! The child node of the pivot
	unique_ptr<BoundTableRef> child;
	//! The number of group columns
	idx_t group_count;
	//! The set of types
	vector<LogicalType> types;
	//! The set of values to pivot on
	vector<PivotValueElement> pivot_values;
};
} // namespace duckdb
