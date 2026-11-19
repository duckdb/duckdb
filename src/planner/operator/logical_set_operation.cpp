#include "duckdb/planner/operator/logical_set_operation.hpp"

#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

LogicalSetOperation::LogicalSetOperation(TableIndex table_index, idx_t column_count, LogicalOperatorType type,
                                         bool setop_all, bool allow_out_of_order)
    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
      allow_out_of_order(allow_out_of_order) {
}

LogicalSetOperation::LogicalSetOperation(TableIndex table_index, idx_t column_count,
                                         vector<unique_ptr<LogicalOperator>> children_p, LogicalOperatorType type,
                                         bool setop_all, bool allow_out_of_order)
    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
      allow_out_of_order(allow_out_of_order) {
	D_ASSERT(type == LogicalOperatorType::LOGICAL_UNION || type == LogicalOperatorType::LOGICAL_EXCEPT ||
	         type == LogicalOperatorType::LOGICAL_INTERSECT);
	children = std::move(children_p);
}

LogicalSetOperation::LogicalSetOperation(TableIndex table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
                                         unique_ptr<LogicalOperator> bottom, LogicalOperatorType type, bool setop_all,
                                         bool allow_out_of_order)
    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
      allow_out_of_order(allow_out_of_order) {
	D_ASSERT(type == LogicalOperatorType::LOGICAL_UNION || type == LogicalOperatorType::LOGICAL_EXCEPT ||
	         type == LogicalOperatorType::LOGICAL_INTERSECT);
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

vector<LogicalType> LogicalSetOperation::GetComparisonTypes(ClientContext &context) {
	D_ASSERT(type == LogicalOperatorType::LOGICAL_EXCEPT || type == LogicalOperatorType::LOGICAL_INTERSECT);
	if (comparison_types.size() == column_count) {
		return comparison_types;
	}
	ResolveOperatorTypes();
	auto &left_types = children[0]->types;
	auto &right_types = children[1]->types;
	D_ASSERT(left_types.size() == column_count && right_types.size() == column_count);
	// the children have the same types (the binder casts them), but a collation can be on one of them only
	vector<LogicalType> result;
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(LogicalType::ForceMaxLogicalType(context, left_types[i], right_types[i]));
	}
	return result;
}

vector<JoinCondition> LogicalSetOperation::GetComparisons(ClientContext &context) {
	auto comparison_types = GetComparisonTypes(context);
	D_ASSERT(comparison_types.size() == column_count);
	auto left_bindings = children[0]->GetColumnBindings();
	auto right_bindings = children[1]->GetColumnBindings();
	vector<JoinCondition> result;
	for (idx_t i = 0; i < column_count; i++) {
		// both sides as the type they are compared as, which is also the type of the column of the set operation
		JoinCondition condition(make_uniq<BoundColumnRefExpression>(comparison_types[i], left_bindings[i]),
		                        make_uniq<BoundColumnRefExpression>(comparison_types[i], right_bindings[i]),
		                        ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		if (i + correlated_column_count < column_count) {
			ExpressionBinder::PushCollation(context, condition.LeftReference(), comparison_types[i]);
			ExpressionBinder::PushCollation(context, condition.RightReference(), comparison_types[i]);
		}
		result.push_back(std::move(condition));
	}
	return result;
}

//! Numbers the rows that are equal in all columns, so that EXCEPT ALL and INTERSECT ALL match the n-th copies of a row
static unique_ptr<LogicalOperator> AddRowNumber(Binder &binder, const vector<LogicalType> &types,
                                                idx_t correlated_column_count, unique_ptr<LogicalOperator> child) {
	auto &context = binder.context;
	auto bindings = child->GetColumnBindings();
	auto row_number = GetBuiltinWindowFunction(context, RowNumberFun::Name, {})->Bind(context);
	row_number->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	row_number->WindowEndMutable() = WindowBoundary::UNBOUNDED_FOLLOWING;
	for (idx_t i = 0; i < types.size(); i++) {
		unique_ptr<Expression> partition = make_uniq<BoundColumnRefExpression>(types[i], bindings[i]);
		if (i + correlated_column_count < types.size()) {
			ExpressionBinder::PushCollation(context, partition, types[i]);
		}
		row_number->PartitionsMutable().push_back(std::move(partition));
	}
	auto window = make_uniq<LogicalWindow>(binder.GenerateTableIndex(), optional_idx());
	window->expressions.push_back(std::move(row_number));
	window->AddChild(std::move(child));
	return std::move(window);
}

unique_ptr<LogicalOperator> LogicalSetOperation::CreateJoin(Binder &binder, const vector<LogicalType> &types,
                                                            TableIndex setop_index, LogicalOperatorType type,
                                                            bool setop_all, unique_ptr<LogicalOperator> left,
                                                            unique_ptr<LogicalOperator> right,
                                                            idx_t correlated_column_count) {
	D_ASSERT(type == LogicalOperatorType::LOGICAL_EXCEPT || type == LogicalOperatorType::LOGICAL_INTERSECT);
	D_ASSERT(correlated_column_count <= types.size());
	auto compared_types = types;
	if (setop_all) {
		left = AddRowNumber(binder, types, correlated_column_count, std::move(left));
		right = AddRowNumber(binder, types, correlated_column_count, std::move(right));
		compared_types.push_back(LogicalType::BIGINT);
	}
	auto left_bindings = left->GetColumnBindings();
	auto right_bindings = right->GetColumnBindings();
	D_ASSERT(left_bindings.size() == compared_types.size() && right_bindings.size() == compared_types.size());

	// the join compares the columns with the collations of both children and treats NULLs as equal. The correlated
	// columns and the row number are compared as they are
	auto join_type = type == LogicalOperatorType::LOGICAL_EXCEPT ? JoinType::ANTI : JoinType::SEMI;
	auto join = make_uniq<LogicalComparisonJoin>(join_type);
	for (idx_t i = 0; i < compared_types.size(); i++) {
		auto &compared_type = compared_types[i];
		JoinCondition condition(make_uniq<BoundColumnRefExpression>(compared_type, left_bindings[i]),
		                        make_uniq<BoundColumnRefExpression>(compared_type, right_bindings[i]),
		                        ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		if (i + correlated_column_count < types.size()) {
			ExpressionBinder::PushCollation(binder.context, condition.LeftReference(), compared_type);
			ExpressionBinder::PushCollation(binder.context, condition.RightReference(), compared_type);
		}
		join->conditions.push_back(std::move(condition));
	}
	join->AddChild(std::move(left));
	join->AddChild(std::move(right));

	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < types.size(); i++) {
		select_list.push_back(make_uniq<BoundColumnRefExpression>(types[i], left_bindings[i]));
	}
	auto projection = make_uniq<LogicalProjection>(setop_index, std::move(select_list));
	projection->AddChild(std::move(join));
	return std::move(projection);
}

vector<TableIndex> LogicalSetOperation::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalSetOperation::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
