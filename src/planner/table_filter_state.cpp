#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

static bool IsSimpleFilterColumnRef(const Expression &expression) {
	return expression.type == ExpressionType::BOUND_REF || expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
}

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) {
	auto initialize_executor = [&]() {
		executor = make_uniq<ExpressionExecutor>(context);
		executor->AddExpression(expression);
	};
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		if (conjunction.type == ExpressionType::CONJUNCTION_AND && !conjunction.children.empty()) {
			child_states.reserve(conjunction.children.size());
			for (auto &child : conjunction.children) {
				child_states.push_back(make_uniq<ExpressionFilterState>(context, *child));
			}
			if (conjunction.children.size() > 1) {
				adaptive_filter = make_uniq<AdaptiveFilter>(expression);
			}
			return;
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = expression.Cast<BoundComparisonExpression>();
		if (IsSimpleFilterColumnRef(*comparison.left) && comparison.right->type == ExpressionType::VALUE_CONSTANT) {
			fast_path = ExpressionFilterFastPath::CONSTANT_COMPARISON;
			comparison_type = comparison.GetExpressionType();
			constant = comparison.right->Cast<BoundConstantExpression>().value;
			initialize_executor();
			return;
		}
		if (IsSimpleFilterColumnRef(*comparison.right) && comparison.left->type == ExpressionType::VALUE_CONSTANT) {
			fast_path = ExpressionFilterFastPath::CONSTANT_COMPARISON;
			comparison_type = FlipComparisonExpression(comparison.GetExpressionType());
			constant = comparison.left->Cast<BoundConstantExpression>().value;
			initialize_executor();
			return;
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op = expression.Cast<BoundOperatorExpression>();
		if (op.children.size() == 1 && IsSimpleFilterColumnRef(*op.children[0])) {
			if (expression.type == ExpressionType::OPERATOR_IS_NULL) {
				fast_path = ExpressionFilterFastPath::IS_NULL;
				initialize_executor();
				return;
			}
			if (expression.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
				fast_path = ExpressionFilterFastPath::IS_NOT_NULL;
				initialize_executor();
				return;
			}
		}
	}
	initialize_executor();
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::BLOOM_FILTER: {
		auto &bf = filter.Cast<BFTableFilter>();
		return make_uniq<BFTableFilterState>(bf.GetKeyType());
	}
	case TableFilterType::OPTIONAL_FILTER: {
		// the optional filter may be executed if it is a SelectivityOptionalFilter
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return optional_filter.InitializeState(context);
	}

	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		return Initialize(context, *struct_filter.child_filter);
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
		auto result = make_uniq<ConjunctionOrFilterState>();
		for (auto &child_filter : conj_filter.child_filters) {
			result->child_states.push_back(Initialize(context, *child_filter));
		}
		return std::move(result);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
		auto result = make_uniq<ConjunctionAndFilterState>();
		for (auto &child_filter : conj_filter.child_filters) {
			result->child_states.push_back(Initialize(context, *child_filter));
		}
		return std::move(result);
	}
	case TableFilterType::EXPRESSION_FILTER: {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
	}
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
	case TableFilterType::PERFECT_HASH_JOIN_FILTER:
	case TableFilterType::PREFIX_RANGE_FILTER:
		// root nodes - create an empty filter state
		return make_uniq<TableFilterState>();
	default:
		throw InternalException("Unsupported filter type for TableFilterState::Initialize");
	}
}

} // namespace duckdb
