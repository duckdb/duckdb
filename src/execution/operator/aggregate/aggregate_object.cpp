#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

AggregateObject::AggregateObject(BoundAggregateFunction function, FunctionData *bind_data, idx_t child_count,
                                 idx_t payload_size, AggregateType aggr_type, PhysicalType return_type,
                                 Expression *filter)
    : function(std::move(function)),
      bind_data_wrapper(bind_data ? make_shared_ptr<FunctionDataWrapper>(bind_data->Copy()) : nullptr),
      child_count(child_count), payload_size(payload_size), aggr_type(aggr_type), return_type(return_type),
      filter(filter) {
}

AggregateObject::AggregateObject(BoundAggregateExpression &aggr)
    : AggregateObject(aggr.function, aggr.bind_info.get(), aggr.children.size(),
                      AlignValue(aggr.function.GetStateSizeCallback()(aggr.function)), aggr.aggr_type,
                      aggr.GetReturnType().InternalType(), aggr.filter.get()) {
}

AggregateObject::AggregateObject(BoundAggregateExpression *aggr) : AggregateObject(*aggr) {
}

AggregateObject::AggregateObject(const BoundWindowExpression &window)
    : AggregateObject(*window.aggregate, window.bind_info.get(), window.children.size(),
                      AlignValue(window.aggregate->GetStateSizeCallback()(*window.aggregate)),
                      window.distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT,
                      window.GetReturnType().InternalType(), window.filter_expr.get()) {
}

vector<AggregateObject> AggregateObject::CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings) {
	vector<AggregateObject> aggregates;
	aggregates.reserve(bindings.size());
	for (auto &binding : bindings) {
		aggregates.emplace_back(binding);
	}
	return aggregates;
}

AggregateFilterData::AggregateFilterData(ClientContext &context, Expression &filter_expr,
                                         const vector<LogicalType> &payload_types)
    : filter_executor(context, &filter_expr), true_sel(STANDARD_VECTOR_SIZE) {
	if (payload_types.empty()) {
		return;
	}
	filtered_payload.Initialize(Allocator::Get(context), payload_types);
}

idx_t AggregateFilterData::ApplyFilter(DataChunk &payload) {
	filtered_payload.Reset();

	auto count = filter_executor.SelectExpression(payload, true_sel);
	filtered_payload.Slice(payload, true_sel, count);
	return count;
}

AggregateFilterDataSet::AggregateFilterDataSet() {
}

void AggregateFilterDataSet::Initialize(ClientContext &context, const vector<AggregateObject> &aggregates,
                                        const vector<LogicalType> &payload_types) {
	bool has_filters = false;
	for (auto &aggregate : aggregates) {
		if (aggregate.filter) {
			has_filters = true;
			break;
		}
	}
	if (!has_filters) {
		// no filters: nothing to do
		return;
	}
	filter_map.resize(aggregates.size());
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = aggregates[aggr_idx];
		if (aggr.filter) {
			for (idx_t other_idx = 0; other_idx < aggr_idx; other_idx++) {
				auto &other = aggregates[other_idx];
				if (other.filter && Expression::Equals(*aggr.filter, *other.filter)) {
					filter_map[aggr_idx] = filter_map[other_idx];
					break;
				}
			}
			if (!filter_map[aggr_idx].IsValid()) {
				filter_map[aggr_idx] = filter_data.size();
				filter_data.push_back(make_uniq<AggregateFilterData>(context, *aggr.filter, payload_types));
			}
		}
	}
	cached_counts.resize(filter_data.size());
}

void AggregateFilterDataSet::BeginChunk() {
	for (auto &count : cached_counts) {
		count.SetInvalid();
	}
}

idx_t AggregateFilterDataSet::ApplyFilter(idx_t aggr_idx, DataChunk &payload) {
	auto filter_idx = filter_map[aggr_idx].GetIndex();
	if (cached_counts[filter_idx].IsValid()) {
		return cached_counts[filter_idx].GetIndex();
	}
	auto count = filter_data[filter_idx]->ApplyFilter(payload);
	cached_counts[filter_idx] = count;
	return count;
}

AggregateFilterData &AggregateFilterDataSet::GetFilterData(idx_t aggr_idx) {
	D_ASSERT(aggr_idx < filter_map.size());
	D_ASSERT(filter_map[aggr_idx].IsValid());
	auto filter_idx = filter_map[aggr_idx].GetIndex();
	D_ASSERT(filter_idx < filter_data.size());
	D_ASSERT(filter_data[filter_idx]);
	return *filter_data[filter_idx];
}
} // namespace duckdb
