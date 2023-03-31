//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_pos_join_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Represents a positional join
class BoundPositionalJoinRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::POSITIONAL_JOIN;

public:
	BoundPositionalJoinRef() : BoundTableRef(TableReferenceType::POSITIONAL_JOIN), lateral(false) {
	}

	//! The binder used to bind the LHS of the positional join
	shared_ptr<Binder> left_binder;
	//! The binder used to bind the RHS of the positional join
	shared_ptr<Binder> right_binder;
	//! The left hand side of the positional join
	unique_ptr<BoundTableRef> left;
	//! The right hand side of the positional join
	unique_ptr<BoundTableRef> right;
	//! Whether or not this is a lateral positional join
	bool lateral;
	//! The correlated columns of the right-side with the left-side
	vector<CorrelatedColumnInfo> correlated_columns;
};
} // namespace duckdb
