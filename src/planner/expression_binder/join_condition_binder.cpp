#include "duckdb/planner/expression_binder/join_condition_binder.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

JoinConditionBinder::JoinConditionBinder(Binder &binder, ClientContext &context,
                                         const unordered_set<TableIndex> &lateral_bindings)
    : WhereBinder(binder, context), lateral_bindings(lateral_bindings) {
}

BindResult JoinConditionBinder::BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	auto expression_class = expr->GetExpressionClass();
	auto result = WhereBinder::BindExpression(expr, depth, root_expression);
	if (result.HasError() || expression_class != ExpressionClass::COLUMN_REF ||
	    result.expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return result;
	}

	auto &bound_column = result.expression->Cast<BoundColumnRefExpression>();
	if (lateral_bindings.find(bound_column.Binding().table_index) == lateral_bindings.end()) {
		return result;
	}
	bound_column.DepthMutable()++;
	if (depth == 0) {
		GetBinder().AddCorrelatedColumn(CorrelatedColumnInfo(bound_column));
	}
	return result;
}

} // namespace duckdb
