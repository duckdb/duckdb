//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_aggregate_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"

namespace duckdb {

struct WindowAggregateStates {
	WindowAggregateStates(ClientContext &client, const AggregateObject &aggr);
	~WindowAggregateStates() {
		Destroy();
	}

	//! The number of states
	idx_t GetCount() const {
		return states.size() / state_size;
	}
	data_ptr_t *GetData() {
		return FlatVector::GetData<data_ptr_t>(*statef);
	}
	data_ptr_t GetStatePtr(idx_t idx) {
		return states.data() + idx * state_size;
	}
	const_data_ptr_t GetStatePtr(idx_t idx) const {
		return states.data() + idx * state_size;
	}
	//! Initialise all the states
	void Initialize(idx_t count);
	//! Combine the states into the target
	void Combine(WindowAggregateStates &target);
	//! Finalize the states into an output vector
	void Finalize(Vector &result);
	//! Destroy the states
	void Destroy();

	//! The context to use for memory etc.
	ClientContext &client;
	//! A description of the aggregator
	const AggregateObject aggr;
	//! The size of each state
	const idx_t state_size;
	//! The allocator to use
	ArenaAllocator allocator;
	//! Data pointer that contains the state data
	vector<data_t> states;
	//! Reused result state container for the window functions
	unique_ptr<Vector> statef;
};

} // namespace duckdb
