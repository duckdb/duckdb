#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {
using namespace std;

PhysicalHashAggregate::PhysicalHashAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(types, move(expressions), {}, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, PhysicalOperatorType type)
    : PhysicalSink(type, types), groups(move(groups_p)) {
	// get a list of all aggregates to be computed
	// fake a single group with a constant value for aggregation without groups
	if (this->groups.size() == 0) {
		auto ce = make_unique<BoundConstantExpression>(Value::TINYINT(42));
		this->groups.push_back(move(ce));
		is_implicit_aggr = true;
	} else {
		is_implicit_aggr = false;
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	all_combinable = true;
	for (auto &expr : expressions) {
		assert(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		assert(expr->IsAggregate());
		auto &aggr = (BoundAggregateExpression &)*expr;
		bindings.push_back(&aggr);

		aggregate_types.push_back(aggr.return_type);
		if (aggr.children.size()) {
			for (idx_t i = 0; i < aggr.children.size(); ++i) {
				payload_types.push_back(aggr.children[i]->return_type);
			}
		} else {
			// COUNT(*)
			payload_types.push_back(LogicalType::BIGINT);
		}
		if (!aggr.function.combine) {
			all_combinable = false;
		}
		aggregates.push_back(move(expr));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalOperatorState {
public:
	HashAggregateGlobalState(BufferManager &buffer_manager, vector<LogicalType> &group_types,
	                         vector<LogicalType> &payload_types, vector<BoundAggregateExpression *> &bindings)
	    : is_empty(true) {
		final_ht = make_unique<GroupedAggregateHashTable>(buffer_manager, STANDARD_VECTOR_SIZE * 2, group_types,
		                                                  payload_types, bindings);
	}

	unique_ptr<GroupedAggregateHashTable> final_ht;
	//! Whether or not any tuples were added to the HT
	bool is_empty;
	//! The lock for updating the global aggregate state
	std::mutex lock;
};

typedef uint64_t radix_t;

class HashAggregateLocalState : public LocalSinkState {
public:
	HashAggregateLocalState(BufferManager &buffer_manager, vector<unique_ptr<Expression>> &groups,
	                        vector<BoundAggregateExpression *> &bound_aggregates, vector<LogicalType> &group_types,
	                        vector<LogicalType> &payload_types)
	    : buffer_manager(buffer_manager), bound_aggregates(bound_aggregates), group_executor(groups) {
		for (auto &aggr : bound_aggregates) {
			if (aggr->children.size()) {
				for (idx_t i = 0; i < aggr->children.size(); ++i) {
					payload_executor.AddExpression(*aggr->children[i]);
				}
			}
		}
		group_chunk.Initialize(group_types);
		if (payload_types.size() > 0) {
			payload_chunk.Initialize(payload_types);
		}
		hts[0].push_back(make_unique<GroupedAggregateHashTable>(buffer_manager, STANDARD_VECTOR_SIZE * 2, group_types,
		                                                        payload_types, bound_aggregates));
	}

	BufferManager &buffer_manager;

	vector<BoundAggregateExpression *> &bound_aggregates;

	//! Expression executor for the GROUP BY chunk
	ExpressionExecutor group_executor;
	//! Expression state for the payload
	ExpressionExecutor payload_executor;
	//! Materialized GROUP BY expression
	DataChunk group_chunk;
	//! The payload chunk
	DataChunk payload_chunk;
	//! The aggregate HT

	unordered_map<radix_t, vector<unique_ptr<GroupedAggregateHashTable>>> hts;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
};

unique_ptr<GlobalOperatorState> PhysicalHashAggregate::GetGlobalState(ClientContext &context) {
	return make_unique<HashAggregateGlobalState>(BufferManager::GetBufferManager(context), group_types, payload_types,
	                                             bindings);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<HashAggregateLocalState>(BufferManager::GetBufferManager(context.client), groups, bindings,
	                                            group_types, payload_types);
}

void PhysicalHashAggregate::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                                 DataChunk &input) {
	auto &llstate = (HashAggregateLocalState &)lstate;

	auto &sink = (HashAggregateLocalState &)lstate;

	DataChunk &group_chunk = sink.group_chunk;
	DataChunk &payload_chunk = sink.payload_chunk;
	sink.group_executor.Execute(input, group_chunk);
	sink.payload_executor.SetChunk(input);

	payload_chunk.Reset();
	idx_t payload_idx = 0, payload_expr_idx = 0;
	payload_chunk.SetCardinality(group_chunk);
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = (BoundAggregateExpression &)*aggregates[i];
		if (aggr.children.size()) {
			for (idx_t j = 0; j < aggr.children.size(); ++j) {
				sink.payload_executor.ExecuteExpression(payload_expr_idx, payload_chunk.data[payload_idx]);
				payload_idx++;
				payload_expr_idx++;
			}
		} else {
			payload_idx++;
		}
	}

	group_chunk.Verify();
	payload_chunk.Verify();
	assert(payload_chunk.column_count() == 0 || group_chunk.size() == payload_chunk.size());

	// intermediate ht

	// 32 bit ht entry
	// 8 bits salt
	// 24 bit payload idx
	// 2^24 = 16777216 or 2^24/32 = 524288 entries max

	// final ht
	// 64 bit ht
	// 16 bit salt
	// 48 bit payload idx

	if (llstate.hts[0].back()->Size() > 50000) /* FIXME what limit */ {
		// FIXME this code is duplicated from llstate constructor
		llstate.hts[0].push_back(make_unique<GroupedAggregateHashTable>(
		    llstate.buffer_manager, STANDARD_VECTOR_SIZE * 2, group_types, payload_types, llstate.bound_aggregates));
	}

	llstate.hts[0].back()->AddChunk(group_chunk, payload_chunk);
	llstate.is_empty = false;
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalHashAggregateState : public PhysicalOperatorState {
public:
	PhysicalHashAggregateState(PhysicalOperator &op, vector<LogicalType> &group_types,
	                           vector<LogicalType> &aggregate_types, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), ht_scan_position(0) {
		group_chunk.Initialize(group_types);
		if (aggregate_types.size() > 0) {
			aggregate_chunk.Initialize(aggregate_types);
		}
	}

	//! Materialized GROUP BY expression
	DataChunk group_chunk;
	//! Materialized aggregates
	DataChunk aggregate_chunk;
	//! The current position to scan the HT for output tuples
	idx_t ht_scan_position;
};

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &source = (HashAggregateLocalState &)lstate;
	assert(all_combinable);

	lock_guard<mutex> glock(gstate.lock);

	gstate.is_empty &= source.is_empty;
	for (auto &ht : source.hts[0]) {
		gstate.final_ht->Combine(*ht);
	}
}

class PhysicalHashAggregateFinalizeTask : public Task {
public:
	PhysicalHashAggregateFinalizeTask(Pipeline &parent_, HashAggregateGlobalState &state_)
	    : parent(parent_), state(state_) {
	}
	void Execute() {
		state.final_ht->Finalize();

		parent.finished_tasks++;
		parent.Finish();
	}

private:
	Pipeline &parent;
	HashAggregateGlobalState &state;
};

void PhysicalHashAggregate::Finalize(Pipeline &pipeline, ClientContext &context,
                                     unique_ptr<GlobalOperatorState> state) {

	this->sink_state = move(state);
	auto gstate = (HashAggregateGlobalState *)this->sink_state.get();
	assert(gstate->final_ht);

	// schedule additional tasks to combine the partial HTs
	pipeline.total_tasks += 1;
	auto t = make_unique<PhysicalHashAggregateFinalizeTask>(pipeline, *gstate);
	TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(t));
}

void PhysicalHashAggregate::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                             PhysicalOperatorState *state_) {
	auto &gstate = (HashAggregateGlobalState &)*sink_state;
	auto &state = (PhysicalHashAggregateState &)*state_;

	state.group_chunk.Reset();
	state.aggregate_chunk.Reset();
	idx_t elements_found = gstate.final_ht->Scan(state.ht_scan_position, state.group_chunk, state.aggregate_chunk);

	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (elements_found == 0 && gstate.is_empty && is_implicit_aggr) {
		assert(chunk.column_count() == aggregates.size());
		// for each column in the aggregates, set to initial state
		chunk.SetCardinality(1);
		for (idx_t i = 0; i < chunk.column_count(); i++) {
			assert(aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregates[i];
			auto aggr_state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(aggr_state.get());

			Vector state_vector(Value::POINTER((uintptr_t)aggr_state.get()));
			aggr.function.finalize(state_vector, chunk.data[i], 1);
		}
		state.finished = true;
		return;
	}
	if (elements_found == 0 && !state.finished) {
		state.finished = true;
		return;
	}
	// compute the final projection list
	idx_t chunk_index = 0;
	chunk.SetCardinality(elements_found);
	if (state.group_chunk.column_count() + state.aggregate_chunk.column_count() == chunk.column_count()) {
		for (idx_t col_idx = 0; col_idx < state.group_chunk.column_count(); col_idx++) {
			chunk.data[chunk_index++].Reference(state.group_chunk.data[col_idx]);
		}
	} else {
		assert(state.aggregate_chunk.column_count() == chunk.column_count());
	}

	for (idx_t col_idx = 0; col_idx < state.aggregate_chunk.column_count(); col_idx++) {
		chunk.data[chunk_index++].Reference(state.aggregate_chunk.data[col_idx]);
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	return make_unique<PhysicalHashAggregateState>(*this, group_types, aggregate_types,
	                                               children.size() == 0 ? nullptr : children[0].get());
}

} // namespace duckdb
