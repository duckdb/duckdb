//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/table_binding_resolver.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {

struct BoundTable {
	index_t table_index;
	index_t column_count;
	index_t column_offset;
};

//! The TableBindingResolver scans a logical tree and constructs TableBindings
class TableBindingResolver : public LogicalOperatorVisitor {
public:
	//! If recurse_into_subqueries is true, we clear the list of tables and recurse when we encounter "blocking node"
	//! A "blocking node" is a node that clears the bound tables (e.g. a LogicalSubquery, LogicalProjection or
	//! LogicalAggregate) The reason this is a "blocking node" is that tables BELOW the blocking node cannot be
	//! referenced ABOVE the blocking node i.e. if we have PROJECTION(AGGREGATE(GET())) the AGGREGATE() blocks the
	//! PROJECTION() from accessing the table in GET()
	TableBindingResolver(bool recurse_into_subqueries = false, bool visit_expressions = false);

	void VisitOperator(LogicalOperator &op) override;
	void VisitExpression(unique_ptr<Expression> *expression) override;

	//! The set of BoundTables found by the resolver
	vector<BoundTable> bound_tables;

protected:
	void Visit(LogicalAggregate &op);
	void Visit(LogicalAnyJoin &op);
	void Visit(LogicalComparisonJoin &op);
	void Visit(LogicalCreateIndex &op);
	void Visit(LogicalDelimGet &op);
	void Visit(LogicalExpressionGet &op);
	void Visit(LogicalEmptyResult &op);
	void Visit(LogicalCrossProduct &op);
	void Visit(LogicalChunkGet &op);
	void Visit(LogicalGet &op);
	void Visit(LogicalIndexScan &op);
	void Visit(LogicalProjection &op);
	void Visit(LogicalSetOperation &op);
	void Visit(LogicalSubquery &op);
	void Visit(LogicalTableFunction &op);
	void Visit(LogicalWindow &op);

	//! Whether or not we should recurse into subqueries
	bool recurse_into_subqueries;
	//! Whether or not we should visit expressions
	bool visit_expressions;

private:
	void PushBinding(BoundTable binding);
	void BindTablesBinaryOp(LogicalOperator &op, bool append_right);
	//! Append a list of tables to the current set of bound tables
	void AppendTables(vector<BoundTable> &right_tables);
	//! Recurse into a subquery
	void RecurseIntoSubquery(LogicalOperator &op);
};
} // namespace duckdb
