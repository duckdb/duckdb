//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class BoundColumnRefExpression;
class TableCatalogEntry;
class ClientContext;

//! JoinElimination is used to remove unnecessary outer joins.
//! The basic idea is that for outer joins, each row of the outer side will definitely appear once or multiple times in
//! the join result set. When the row of the outer side cannot find a match or can only find one matching row, this row
//! of the outer side will only appear once in the join result; When the row of the outer side can find multiple
//! matching rows, it will appear multiple times in the join result. Therefore, if the inner side satisfies the
//! uniqueness property on the join key, it is impossible for the rows of the outer side to find multiple matching rows.
//! So each row of the outer side will only appear once in the join result.
//! At the same time, if the upper-level(father) operator only needs the data of the outer side, the outer join can be
//! directly eliminated from the query.
//! TODO: Similarly, it is easy to understand that when the upper-level(father) operator only needs the deduplicated
//! result of the outer side, the outer join can also be eliminated.
class JoinElimination : public LogicalOperatorVisitor {
public:
	JoinElimination(ClientContext &context) : context(context), finish_collection(false) {
	}

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	void CollectUniqueConstraintsSet(LogicalOperator &op);

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	ClientContext &context;
	//! The set of column references
	column_binding_set_t column_references;
	//! Store the unique constraints set for the query
	vector<column_binding_set_t> unique_constraints_set;
	bool finish_collection;
};
} // namespace duckdb
