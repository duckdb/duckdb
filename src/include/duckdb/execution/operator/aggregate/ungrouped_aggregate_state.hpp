//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct UngroupedAggregateState {
	explicit UngroupedAggregateState(const vector<unique_ptr<Expression>> &aggregate_expressions) :
		aggregate_expressions(aggregate_expressions) {
		counts = make_uniq_array<atomic<idx_t>>(aggregate_expressions.size());
		for (idx_t i = 0; i < aggregate_expressions.size(); i++) {
			auto &aggregate = aggregate_expressions[i];
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			auto state = make_unsafe_uniq_array<data_t>(aggr.function.state_size());
			aggr.function.initialize(state.get());
			aggregate_data.push_back(std::move(state));
			bind_data.push_back(aggr.bind_info.get());
			destructors.push_back(aggr.function.destructor);
#ifdef DEBUG
			counts[i] = 0;
#endif
		}
	}
	~UngroupedAggregateState() {
		D_ASSERT(destructors.size() == aggregate_data.size());
		for (idx_t i = 0; i < destructors.size(); i++) {
			if (!destructors[i]) {
				continue;
			}
			Vector state_vector(Value::POINTER(CastPointerToValue(aggregate_data[i].get())));
			state_vector.SetVectorType(VectorType::FLAT_VECTOR);

			ArenaAllocator allocator(Allocator::DefaultAllocator());
			AggregateInputData aggr_input_data(bind_data[i], allocator);
			destructors[i](state_vector, aggr_input_data, 1);
		}
	}

	void Move(UngroupedAggregateState &other) {
		other.aggregate_data = std::move(aggregate_data);
		other.destructors = std::move(destructors);
	}

	//! Aggregates
	const vector<unique_ptr<Expression>> &aggregate_expressions;
	//! The aggregate values
	vector<unsafe_unique_array<data_t>> aggregate_data;
	//! The bind data
	vector<optional_ptr<FunctionData>> bind_data;
	//! The destructors
	vector<aggregate_destructor_t> destructors;
	//! Counts (used for verification)
	unique_array<atomic<idx_t>> counts;
};


} // namespace duckdb
