#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/partitionable_hashtable.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
using namespace std;

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions, PhysicalOperatorType type)
    : PhysicalHashAggregate(context, types, move(expressions), {}, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, PhysicalOperatorType type)
    : PhysicalSink(type, types), groups(move(groups_p)), all_combinable(true), any_distinct(false) {
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

	for (auto &expr : expressions) {
		assert(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		assert(expr->IsAggregate());
		auto &aggr = (BoundAggregateExpression &)*expr;
		bindings.push_back(&aggr);

		if (aggr.distinct) {
			any_distinct = true;
		}

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

	// 10000 seems like a good compromise here
	radix_limit = 10000;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalOperatorState {
public:
	HashAggregateGlobalState(PhysicalHashAggregate &_op, ClientContext &context)
	    : op(_op), is_empty(true), lossy_total_groups(0),
	      partition_info((idx_t)TaskScheduler::GetScheduler(context).NumberOfThreads()) {
	}

	PhysicalHashAggregate &op;
	vector<unique_ptr<PartitionableHashTable>> intermediate_hts;
	vector<unique_ptr<GroupedAggregateHashTable>> finalized_hts;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
	//! The lock for updating the global aggregate state
	std::mutex lock;
	//! a counter to determine if we should switch over to p
	idx_t lossy_total_groups;

	RadixPartitionInfo partition_info;
};

class HashAggregateLocalState : public LocalSinkState {
public:
	HashAggregateLocalState(PhysicalHashAggregate &_op) : op(_op), group_executor(op.groups), is_empty(true) {
		for (auto &aggr : op.bindings) {
			if (aggr->children.size()) {
				for (idx_t i = 0; i < aggr->children.size(); ++i) {
					payload_executor.AddExpression(*aggr->children[i]);
				}
			}
		}
		group_chunk.Initialize(op.group_types);
		if (op.payload_types.size() > 0) {
			payload_chunk.Initialize(op.payload_types);
		}
	}

	PhysicalHashAggregate &op;

	//! Expression executor for the GROUP BY chunk
	ExpressionExecutor group_executor;
	//! Expression state for the payload
	ExpressionExecutor payload_executor;
	//! Materialized GROUP BY expression
	DataChunk group_chunk;
	//! The payload chunk
	DataChunk payload_chunk;
	//! The aggregate HT

	unique_ptr<PartitionableHashTable> ht;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
};

unique_ptr<GlobalOperatorState> PhysicalHashAggregate::GetGlobalState(ClientContext &context) {
	return make_unique<HashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<HashAggregateLocalState>(*this);
}

void PhysicalHashAggregate::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                                 DataChunk &input) {
	auto &llstate = (HashAggregateLocalState &)lstate;
	auto &gstate = (HashAggregateGlobalState &)state;

	DataChunk &group_chunk = llstate.group_chunk;
	DataChunk &payload_chunk = llstate.payload_chunk;
	llstate.group_executor.Execute(input, group_chunk);
	llstate.payload_executor.SetChunk(input);

	payload_chunk.Reset();
	idx_t payload_idx = 0, payload_expr_idx = 0;
	payload_chunk.SetCardinality(group_chunk);
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = (BoundAggregateExpression &)*aggregates[i];
		if (aggr.children.size()) {
			for (idx_t j = 0; j < aggr.children.size(); ++j) {
				llstate.payload_executor.ExecuteExpression(payload_expr_idx, payload_chunk.data[payload_idx]);
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

	// if we have non-combinable aggregates (e.g. string_agg) or any distinct aggregates we cannot keep parallel hash
	// tables
	if (!all_combinable || any_distinct) {
		lock_guard<mutex> glock(gstate.lock);
		gstate.is_empty = gstate.is_empty && group_chunk.size() == 0;
		if (gstate.finalized_hts.size() == 0) {
			gstate.finalized_hts.push_back(
			    make_unique<GroupedAggregateHashTable>(BufferManager::GetBufferManager(context.client), group_types,
			                                           payload_types, bindings, HtEntryType::HT_WIDTH_64));
		}
		assert(gstate.finalized_hts.size() == 1);
		gstate.lossy_total_groups += gstate.finalized_hts[0]->AddChunk(group_chunk, payload_chunk);
		return;
	}

	assert(all_combinable);
	assert(!any_distinct);

	if (group_chunk.size() > 0) {
		llstate.is_empty = false;
	}

	if (!llstate.ht) {
		llstate.ht = make_unique<PartitionableHashTable>(BufferManager::GetBufferManager(context.client),
		                                                 gstate.partition_info, group_types, payload_types, bindings);
	}

	gstate.lossy_total_groups +=
	    llstate.ht->AddChunk(group_chunk, payload_chunk, gstate.lossy_total_groups > radix_limit);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalHashAggregateState : public PhysicalOperatorState {
public:
	PhysicalHashAggregateState(PhysicalOperator &op, vector<LogicalType> &group_types,
	                           vector<LogicalType> &aggregate_types, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), ht_index(0), ht_scan_position(0) {
		auto scan_chunk_types = group_types;
		for (auto &aggr_type : aggregate_types) {
			scan_chunk_types.push_back(aggr_type);
		}
		scan_chunk.Initialize(scan_chunk_types);
	}

	//! Materialized GROUP BY expressions & aggregates
	DataChunk scan_chunk;

	//! The current position to scan the HT for output tuples
	idx_t ht_index;
	idx_t ht_scan_position;
};

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalState &)lstate;

	// this actually does not do a lot but just pushes the local HTs into the global state so we can later combine them
	// in parallel

	if (!all_combinable || any_distinct) {
		assert(gstate.finalized_hts.size() <= 1);
		return;
	}

	if (!llstate.ht) {
		return; // no data
	}

	if (!llstate.ht->IsPartitioned() && gstate.partition_info.radix_partitions > 1 &&
	    gstate.lossy_total_groups > radix_limit) {
		llstate.ht->Partition();
	}

	lock_guard<mutex> glock(gstate.lock);
	assert(all_combinable);
	assert(!any_distinct);

	if (!llstate.is_empty) {
		gstate.is_empty = false;
	}

	// at this point we just collect them the PhysicalHashAggregateFinalizeTask (below) will merge them in parallel
	gstate.intermediate_hts.push_back(move(llstate.ht));
}

// this task is run in multiple threads and combines the radix-partitioned hash tables into a single onen and then
// folds them into the global ht finally.
class PhysicalHashAggregateFinalizeTask : public Task {
public:
	PhysicalHashAggregateFinalizeTask(Pipeline &parent_, HashAggregateGlobalState &state_, idx_t radix_)
	    : parent(parent_), state(state_), radix(radix_) {
	}
	static void FinalizeHT(HashAggregateGlobalState &gstate, idx_t radix) {
		assert(gstate.finalized_hts[radix]);
		for (auto &pht : gstate.intermediate_hts) {
			for (auto &ht : pht->GetPartition(radix)) {
				gstate.finalized_hts[radix]->Combine(*ht);
				ht.reset();
			}
		}
	}

	void Execute() {
		FinalizeHT(state, radix);
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
		// finish the whole pipeline
		if (parent.total_tasks == parent.finished_tasks) {
			parent.Finish();
		}
	}

private:
	Pipeline &parent;
	HashAggregateGlobalState &state;
	idx_t radix;
};

void PhysicalHashAggregate::Finalize(Pipeline &pipeline, ClientContext &context,
                                     unique_ptr<GlobalOperatorState> state) {
	FinalizeInternal(context, move(state), false, &pipeline);
}

void PhysicalHashAggregate::FinalizeImmediate(ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	FinalizeInternal(context, move(state), true, nullptr);
}

void PhysicalHashAggregate::FinalizeInternal(ClientContext &context, unique_ptr<GlobalOperatorState> state,
                                             bool immediate, Pipeline *pipeline) {
	this->sink_state = move(state);
	auto &gstate = (HashAggregateGlobalState &)*this->sink_state;

	// special case if we have non-combinable aggregates
	// we have already aggreagted into a global shared HT that does not require any additional finalization steps
	if (!all_combinable || any_distinct) {
		assert(gstate.finalized_hts.size() <= 1);
		return;
	}

	// we can have two cases now, non-partitioned for few groups and radix-partitioned for very many groups.
	// go through all of the child hts and see if we ever called partition() on any of them
	// if we did, its the latter case.
	bool any_partitioned = false;
	for (auto &pht : gstate.intermediate_hts) {
		if (pht->IsPartitioned()) {
			any_partitioned = true;
			break;
		}
	}

	if (any_partitioned) {
		// if one is partitioned, all have to be
		// this should mostly have already happened in Combine, but if not we do it here
		for (auto &pht : gstate.intermediate_hts) {
			if (!pht->IsPartitioned()) {
				pht->Partition();
			}
		}
		// schedule additional tasks to combine the partial HTs
		if (!immediate) {
			assert(pipeline);
			pipeline->total_tasks += gstate.partition_info.radix_partitions;
		}
		gstate.finalized_hts.resize(gstate.partition_info.radix_partitions);
		for (idx_t r = 0; r < gstate.partition_info.radix_partitions; r++) {
			// TODO possible optimization, if total count < limit for 32 bit ht, use that one
			// create this ht here so finalize needs no lock on gstate
			gstate.finalized_hts[r] =
			    make_unique<GroupedAggregateHashTable>(BufferManager::GetBufferManager(context), group_types,
			                                           payload_types, bindings, HtEntryType::HT_WIDTH_64);
			if (immediate) {
				PhysicalHashAggregateFinalizeTask::FinalizeHT(gstate, r);
			} else {
				assert(pipeline);
				auto new_task = make_unique<PhysicalHashAggregateFinalizeTask>(*pipeline, gstate, r);
				TaskScheduler::GetScheduler(context).ScheduleTask(pipeline->token, move(new_task));
			}
		}
	} else { // in the non-partitioned case we immediately combine all the unpartitioned hts created by the threads.
		gstate.finalized_hts.push_back(make_unique<GroupedAggregateHashTable>(
		    BufferManager::GetBufferManager(context), group_types, payload_types, bindings, HtEntryType::HT_WIDTH_64));
		for (auto &pht : gstate.intermediate_hts) {
			auto unpartitioned = pht->GetUnpartitioned();
			assert(unpartitioned);
			gstate.finalized_hts[0]->Combine(*unpartitioned);
			unpartitioned.reset();
		}
	}
}

void PhysicalHashAggregate::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                             PhysicalOperatorState *state_) {
	auto &gstate = (HashAggregateGlobalState &)*sink_state;
	auto &state = (PhysicalHashAggregateState &)*state_;

	state.scan_chunk.Reset();

	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (gstate.is_empty && is_implicit_aggr) {
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
			if (aggr.function.destructor) {
				aggr.function.destructor(state_vector, 1);
			}
		}
		state.finished = true;
		return;
	}
	if (gstate.is_empty && !state.finished) {
		state.finished = true;
		return;
	}
	idx_t elements_found = 0;

	while (true) {
		if (state.ht_index == gstate.finalized_hts.size()) {
			state.finished = true;
			return;
		}
		elements_found = gstate.finalized_hts[state.ht_index]->Scan(state.ht_scan_position, state.scan_chunk);

		if (elements_found > 0) {
			break;
		}
		state.ht_index++;
		state.ht_scan_position = 0;
	}

	// compute the final projection list
	idx_t chunk_index = 0;
	chunk.SetCardinality(elements_found);
	if (group_types.size() + aggregates.size() == chunk.column_count()) {
		for (idx_t col_idx = 0; col_idx < group_types.size(); col_idx++) {
			chunk.data[chunk_index++].Reference(state.scan_chunk.data[col_idx]);
		}
	} else {
		assert(aggregates.size() == chunk.column_count());
	}

	for (idx_t col_idx = 0; col_idx < aggregates.size(); col_idx++) {
		chunk.data[chunk_index++].Reference(state.scan_chunk.data[group_types.size() + col_idx]);
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	return make_unique<PhysicalHashAggregateState>(*this, group_types, aggregate_types,
	                                               children.size() == 0 ? nullptr : children[0].get());
}

} // namespace duckdb
