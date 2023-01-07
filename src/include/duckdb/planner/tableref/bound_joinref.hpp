//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! Represents a join
class BoundJoinRef : public BoundTableRef {
public:
	BoundJoinRef() : BoundTableRef(TableReferenceType::JOIN), lateral(false) {
	}

	//! The binder used to bind the LHS of the join
	shared_ptr<Binder> left_binder;
	//! The binder used to bind the RHS of the join
	shared_ptr<Binder> right_binder;
	//! The left hand side of the join
	unique_ptr<BoundTableRef> left;
	//! The right hand side of the join
	unique_ptr<BoundTableRef> right;
	//! The join condition
	unique_ptr<Expression> condition;
	//! The join type
	JoinType type;
	//! Whether or not this is a lateral join
	bool lateral;
	//! The correlated columns of the right-side with the left-side
	vector<CorrelatedColumnInfo> correlated_columns;
};
} // namespace duckdb
