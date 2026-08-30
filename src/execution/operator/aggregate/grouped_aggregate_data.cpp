#include "duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

idx_t GroupedAggregateData::GroupCount() const {
	return groups.size();
}

const vector<vector<ProjectionIndex>> &GroupedAggregateData::GetGroupingFunctions() const {
	return grouping_functions;
}

void GroupedAggregateData::InitializeGroupby(vector<unique_ptr<Expression>> groups,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unsafe_vector<ProjectionIndex>> grouping_functions) {
	InitializeGroupbyGroups(std::move(groups));
	vector<LogicalType> payload_types_filters;

	SetGroupingFunctions(grouping_functions);

	filter_count = 0;
	for (auto &expr : expressions) {
		D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		bindings.push_back(&aggr);

		aggregate_return_types.push_back(aggr.GetReturnType());
		for (auto &child : aggr.GetChildren()) {
			payload_types.push_back(child->GetReturnType());
		}
		if (aggr.GetFilter()) {
			filter_count++;
			payload_types_filters.push_back(aggr.GetFilter()->GetReturnType());
		}
		if (!aggr.Function().HasStateCombineCallback()) {
			throw InternalException("Aggregate function %s is missing a combine method", aggr.Function().GetName());
		}
		aggregates.push_back(std::move(expr));
	}
	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}
}

void GroupedAggregateData::InitializeDistinct(ClientContext &context, const unique_ptr<Expression> &aggregate,
                                              const vector<unique_ptr<Expression>> *groups_p,
                                              const vector<unique_ptr<Expression>> &key_normalizers,
                                              const vector<bool> &key_requires_normalization) {
	auto &aggr = aggregate->Cast<BoundAggregateExpression>();
	D_ASSERT(aggr.IsDistinct());
	D_ASSERT(aggr.GetChildren().size() == key_normalizers.size());
	D_ASSERT(key_normalizers.size() == key_requires_normalization.size());

	// Add the (empty in ungrouped case) groups of the aggregates
	InitializeDistinctGroups(groups_p);
	const auto distinct_key_offset = groups.size();
	bool requires_normalization = false;
	for (const auto requires_normalization_child : key_requires_normalization) {
		if (requires_normalization_child) {
			requires_normalization = true;
			break;
		}
	}

	// bindings.push_back(&aggr);
	filter_count = 0;
	for (idx_t child_idx = 0; child_idx < key_normalizers.size(); child_idx++) {
		auto &normalizer = key_normalizers[child_idx];
		group_types.push_back(normalizer->GetReturnType());
		if (requires_normalization) {
			groups.push_back(
			    make_uniq<BoundReferenceExpression>(normalizer->GetReturnType(), distinct_key_offset + child_idx));
		} else {
			groups.push_back(normalizer->Copy());
		}
	}

	FunctionBinder function_binder(context);
	for (idx_t child_idx = 0; child_idx < aggr.GetChildren().size(); child_idx++) {
		if (!key_requires_normalization[child_idx]) {
			distinct_representative_indices.push_back(distinct_key_offset + child_idx);
			continue;
		}

		auto &child = aggr.GetChildren()[child_idx];
		// The hidden first(...) aggregate reads the original value from this payload column.
		vector<unique_ptr<Expression>> children;
		children.push_back(make_uniq<BoundReferenceExpression>(child->GetReturnType(), payload_types.size()));
		auto representative = function_binder.BindAggregateFunction(
		    FirstFunctionGetter::GetFunction(child->GetReturnType()), std::move(children));

		distinct_representative_indices.push_back(GroupCount() + aggregates.size());
		payload_types.push_back(child->GetReturnType());
		aggregate_return_types.push_back(representative->GetReturnType());
		bindings.push_back(representative.get());
		aggregates.push_back(std::move(representative));
	}
	if (!aggr.Function().HasStateCombineCallback()) {
		throw InternalException("Aggregate function %s is missing a combine method", aggr.Function().GetName());
	}
}

void GroupedAggregateData::InitializeDistinctGroups(const vector<unique_ptr<Expression>> *groups_p) {
	if (!groups_p) {
		return;
	}
	for (auto &expr : *groups_p) {
		group_types.push_back(expr->GetReturnType());
		groups.push_back(expr->Copy());
	}
}

void GroupedAggregateData::InitializeGroupbyGroups(vector<unique_ptr<Expression>> groups) {
	// Add all the expressions of the group by clause
	for (auto &expr : groups) {
		group_types.push_back(expr->GetReturnType());
	}
	this->groups = std::move(groups);
}

void GroupedAggregateData::SetGroupingFunctions(vector<unsafe_vector<ProjectionIndex>> &functions) {
	grouping_functions.reserve(functions.size());
	for (idx_t i = 0; i < functions.size(); i++) {
		grouping_functions.push_back(std::move(functions[i]));
	}
}

} // namespace duckdb
