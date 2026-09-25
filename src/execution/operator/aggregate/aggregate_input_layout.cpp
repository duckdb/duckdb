#include "duckdb/execution/operator/aggregate/aggregate_input_layout.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

AggregateInputLayout::AggregateInputLayout(const vector<BoundAggregateExpression *> &bindings) {
	vector<LogicalType> types;
	vector<idx_t> argument_counts;
	vector<bool> has_filters;
	for (auto &aggregate : bindings) {
		argument_counts.push_back(aggregate->GetChildren().size());
		has_filters.push_back(bool(aggregate->GetFilter()));
		for (auto &child : aggregate->GetChildren()) {
			types.push_back(child->GetReturnType());
		}
	}
	for (auto &aggregate : bindings) {
		if (aggregate->GetFilter()) {
			types.push_back(aggregate->GetFilter()->GetReturnType());
		}
	}
	Initialize(types, argument_counts, has_filters);
}

AggregateInputLayout::AggregateInputLayout(const vector<LogicalType> &types,
                                           const vector<AggregateObject> &aggregates) {
	vector<idx_t> argument_counts;
	vector<bool> has_filters;
	for (auto &aggregate : aggregates) {
		argument_counts.push_back(aggregate.child_count);
		has_filters.push_back(bool(aggregate.filter));
	}
	Initialize(types, argument_counts, has_filters);
}

void AggregateInputLayout::Initialize(const vector<LogicalType> &types, const vector<idx_t> &argument_counts,
                                      const vector<bool> &has_filters) {
	ChunkLayoutBuilder builder;
	idx_t column = 0;
	for (auto count : argument_counts) {
		if (count > types.size() - column) {
			throw InternalException("Aggregate arguments exceed payload column count");
		}
		vector<LogicalType> argument_types(types.begin() + column, types.begin() + column + count);
		arguments.push_back(builder.AddColumns(argument_types));
		column += count;
	}
	for (auto has_filter : has_filters) {
		if (!has_filter) {
			filters.emplace_back();
			continue;
		}
		if (column >= types.size() || types[column] != LogicalType::BOOLEAN) {
			throw InternalException("Aggregate filter requires a boolean payload column");
		}
		filters.emplace_back(column);
		builder.AddColumn(types[column++]);
	}
	// DISTINCT tables without aggregate states can retain unused payload columns.
	if (!argument_counts.empty() && column != types.size()) {
		throw InternalException("Aggregate layout does not cover the payload");
	}
	builder.AddColumns(vector<LogicalType>(types.begin() + column, types.end()));
	layout = make_uniq<ChunkLayout>(builder.Build());
}

ChunkProjection AggregateInputLayout::CreateProjection(const vector<LogicalType> &input_types,
                                                       const vector<BoundAggregateExpression *> &bindings) const {
	ChunkLayoutBuilder builder;
	auto input = builder.AddColumns(input_types);
	auto input_layout = builder.Build();
	vector<ChunkColumn> columns;
	for (auto &aggregate : bindings) {
		for (auto &child : aggregate->GetChildren()) {
			columns.push_back(input.Column(child->Cast<BoundReferenceExpression>().Index()));
		}
	}
	for (auto &aggregate : bindings) {
		if (aggregate->GetFilter()) {
			columns.push_back(input.Column(aggregate->GetFilter()->Cast<BoundReferenceExpression>().Index()));
		}
	}
	return ChunkProjection(std::move(input_layout), *layout, std::move(columns));
}

} // namespace duckdb
