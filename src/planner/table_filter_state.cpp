#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

namespace duckdb {
class SelectivityOptionalFilter;

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) : executor(context) {
	executor.AddExpression(expression);
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::SELECTIVITY_OPTIONAL_FILTER: {
		auto &sel_opt_filter = filter.Cast<SelectivityOptionalFilter>();
		auto child_filter_state = Initialize(context, *sel_opt_filter.child_filter);
		return make_uniq<SelectivityOptionalFilterState>(std::move(child_filter_state));
	}
	case TableFilterType::BLOOM_FILTER: {
		auto &bf = filter.Cast<BFTableFilter>();
		return make_uniq<BFTableFilterState>(bf.GetKeyType());
	}
	case TableFilterType::OPTIONAL_FILTER:
		// optional filter is not executed - create an empty filter state
		return make_uniq<TableFilterState>();
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
		// root nodes - create an empty filter state
		return make_uniq<TableFilterState>();
	default:
		throw InternalException("Unsupported filter type for TableFilterState::Initialize");
	}
}

} // namespace duckdb
