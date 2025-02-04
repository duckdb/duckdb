#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

column_t WindowSharedExpressions::RegisterExpr(const unique_ptr<Expression> &expr, Shared &shared) {
	auto pexpr = expr.get();
	if (!pexpr) {
		return DConstants::INVALID_INDEX;
	}

	//	We need to make separate columns for volatile arguments
	const auto is_volatile = expr->IsVolatile();
	auto i = shared.columns.find(*pexpr);
	if (i != shared.columns.end() && !is_volatile) {
		return i->second.front();
	}

	// New column, find maximum column number
	column_t result = shared.size++;
	shared.columns[*pexpr].emplace_back(result);

	return result;
}

vector<optional_ptr<const Expression>> WindowSharedExpressions::GetSortedExpressions(Shared &shared) {
	vector<optional_ptr<const Expression>> sorted(shared.size);
	for (auto &col : shared.columns) {
		auto &expr = col.first.get();
		for (auto col_idx : col.second) {
			sorted[col_idx] = &expr;
		}
	}

	return sorted;
}
void WindowSharedExpressions::PrepareExecutors(Shared &shared, ExpressionExecutor &exec, DataChunk &chunk) {
	const auto sorted = GetSortedExpressions(shared);
	vector<LogicalType> types;
	for (auto expr : sorted) {
		exec.AddExpression(*expr);
		types.emplace_back(expr->return_type);
	}

	if (!types.empty()) {
		chunk.Initialize(exec.GetAllocator(), types);
	}
}

} // namespace duckdb
