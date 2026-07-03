#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/execution/expression_executor/bitmap_comparison.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

static void InitializeExecutor(ClientContext &context, const Expression &expression, ExpressionFilterState &state) {
	state.executor = make_uniq<ExpressionExecutor>(context);
	state.executor->AddExpression(expression);
}

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression)
    : bitmap_capable(IsBitmapSelectCandidate(expression)) {
	InitializeExecutor(context, expression, *this);
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "TableFilterState::Initialize");
	return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
}

} // namespace duckdb
