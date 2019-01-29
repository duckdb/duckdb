//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/column_binding_resolver.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {

struct BoundTable {
	size_t table_index;
	size_t column_count;
	size_t column_offset;
};

//! The ColumnBindingResolver resolves ColumnBindings into base tables
//! (table_index, column_index) into physical indices into the DataChunks that
//! are used within the execution engine
class ColumnBindingResolver : public LogicalOperatorVisitor {
public:
	ColumnBindingResolver() {
	}

	//! We overide the VisitOperator method because we don't want to automatically visit children of all operators
	void VisitOperator(LogicalOperator &op) override;

protected:
	void Visit(LogicalAggregate &op);
	void Visit(LogicalCreateIndex &op);
	void Visit(LogicalUnion &op);
	void Visit(LogicalExcept &op);
	void Visit(LogicalIntersect &op);
	void Visit(LogicalCrossProduct &op);
	void Visit(LogicalGet &op);
	void Visit(LogicalJoin &op);
	// void Visit(LogicalProjection &op);
	void Visit(LogicalSubquery &op);
	void Visit(LogicalTableFunction &op);

	using SQLNodeVisitor::Visit;
	void Visit(ColumnRefExpression &expr) override {
		throw Exception(
		    "ColumnRefExpression is not legal here, should have been converted to BoundColumnRefExpression already!");
	}
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	// void Visit(BoundSubqueryExpression &expr) override;

	vector<BoundTable> bound_tables;
private:
	void BindTablesBinaryOp(LogicalOperator &op, bool append_right);
	//! Append a list of tables to the current set of bound tables
	void AppendTables(vector<BoundTable> &right_tables);

	void ResolveSubquery(LogicalOperator &op, size_t table_index, size_t column_count);
};
} // namespace duckdb
