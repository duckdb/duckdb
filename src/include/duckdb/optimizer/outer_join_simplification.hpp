//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_join_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class Expression;
struct JoinCondition;
class LogicalAggregate;
class LogicalComparisonJoin;
class LogicalFilter;
class LogicalJoin;
class LogicalOrder;
class LogicalProjection;
class LogicalTopN;

//! Simplifies outer joins if NULL-extended rows are filtered in a way that changes the join semantics
class OuterJoinSimplification : public LogicalOperatorVisitor {
public:
	OuterJoinSimplification();

public:
	void VisitOperator(LogicalOperator &op) override;

private:
	explicit OuterJoinSimplification(column_binding_set_t required_columns);

	//! Extract and propagate column references through expressions and join conditions
	void AddColumnReferences(const Expression &expr, column_binding_set_t &bindings);
	void AddRequiredColumns(const Expression &expr);
	void AddRequiredColumns(const JoinCondition &condition);
	void AddRequiredColumns(const vector<JoinCondition> &conditions);
	bool GetColumnBinding(const Expression &expr, ColumnBinding &binding);
	bool GetNullPreservingColumnBinding(const Expression &expr, ColumnBinding &binding);

	//! Track predicates that require columns to be NULL or reject NULL values
	void HandleExpression(const Expression &expr);
	void HandleFilterExpression(const Expression &expr);
	bool HandleIsNullExpression(const Expression &expr);
	bool IsNullFilter(const Expression &expr, ColumnBinding &binding);
	void InitializeRequiredColumns(LogicalOperator &op);
	bool FiltersNulls(ExpressionType comparison_type);

	//! Rewrite joins based on the NULL constraints collected from operators above them
	bool TryConvertLeftToAntiJoin(LogicalComparisonJoin &join);
	bool HasNullRequiredColumns(const vector<ColumnBinding> &bindings);
	bool HasRequiredColumns(const vector<ColumnBinding> &bindings);
	void MarkEliminatedNullColumns(const vector<ColumnBinding> &bindings);
	vector<ColumnBinding> GetRightBindings(LogicalJoin &join);
	void SimplifyOuterJoinType(LogicalComparisonJoin &join);

	//! Visit operators while preserving only the constraints that can safely pass through each operator type
	void VisitComparisonJoin(LogicalComparisonJoin &join, LogicalOperator &op);
	void VisitInnerOrSemiJoin(LogicalComparisonJoin &join, LogicalOperator &op);
	void VisitOuterJoin(LogicalComparisonJoin &join, LogicalOperator &op);
	void VisitProjection(LogicalProjection &projection, LogicalOperator &op);
	void VisitFilter(LogicalFilter &filter, LogicalOperator &op);
	void VisitOrder(LogicalOrder &order, LogicalOperator &op);
	void VisitTopN(LogicalTopN &top_n, LogicalOperator &op);
	void VisitAggregate(LogicalAggregate &aggregate, LogicalOperator &op);
	void VisitUnsupportedOperator(LogicalOperator &op);

private:
	//! Columns that have their NULL values filtered
	column_binding_set_t null_filtered_columns;
	//! Columns that are required to be NULL
	column_binding_set_t null_required_columns;
	//! Columns that are known to be NULL after rewriting a LEFT join to an ANTI join
	column_binding_set_t eliminated_null_columns;
	//! Columns that must remain available to operators above the current point in the plan
	column_binding_set_t required_columns;
	bool initialized_required_columns = false;
};

} // namespace duckdb
