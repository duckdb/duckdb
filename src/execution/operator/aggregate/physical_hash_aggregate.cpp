#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

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

struct PartitionableHashTable;

struct HashTableList {

	vector<unique_ptr<GroupedAggregateHashTable>> hts;

	idx_t AddChunk(PartitionableHashTable &pht, DataChunk &groups, Vector &group_hashes, DataChunk &payload) {
		if (hts.empty() || hts.back()->Size() > 100000 /* TODO ht_load_limit */) {
			NewHT(pht);
		}
		return hts.back()->AddChunk(groups, group_hashes, payload);
	}

	void CombineRadix(PartitionableHashTable &pht, GroupedAggregateHashTable &other, hash_t radix_mask) {
		NewHT(pht);
		hts.back()->Combine(other);
	}

	void NewHT(PartitionableHashTable &pht);
};

struct PartitionableHashTable {
	PartitionableHashTable(PhysicalHashAggregate &_pha)
	    : pha(_pha), is_partitioned(false), radix_bits(0), radix_mask(0) {

		unpartitioned_ht = make_unique<GroupedAggregateHashTable>(
		    pha.buffer_manager, pha.group_types, pha.payload_types, pha.bindings, HtEntryType::HT_WIDTH_64);

		// finalize_threads needs to be a power of 2
		assert(pha.radix_partitions > 0);
		assert(pha.radix_partitions <= 256);
		assert((pha.radix_partitions & (pha.radix_partitions - 1)) == 0);

		auto radix_partitions_copy = pha.radix_partitions;
		while (radix_partitions_copy - 1) {
			radix_bits++;
			radix_partitions_copy >>= 1;
		}

		assert(radix_bits <= 8);

		// we use the fourth byte of the 64 bit hash as radix source
		radix_mask = 0;
		for (idx_t i = 0; i < radix_bits; i++) {
			radix_mask = (radix_mask << 1) | 1;
		}
		radix_mask <<= 32;
	}

	PhysicalHashAggregate &pha;
	HtEntryType entry_type;
	unique_ptr<GroupedAggregateHashTable> unpartitioned_ht;

	unordered_map<hash_t, HashTableList> radix_partitioned_hts;

	//! how many bits are used for the radix partitions
	idx_t radix_bits;
	//! bit mask to get radix partition
	hash_t radix_mask;

	bool is_partitioned;

	idx_t AddChunk(DataChunk &groups, DataChunk &payload) {
		Vector hashes(LogicalType::HASH);
		groups.Hash(hashes);

		if (is_partitioned) {
			// makes no sense to do this with 1 partition
			assert(pha.radix_partitions > 1);

			Vector radix_mask_vec(Value::HASH(radix_mask));
			vector<SelectionVector> sel_vectors(pha.radix_partitions);
			vector<idx_t> sel_vector_sizes(pha.radix_partitions);
			for (hash_t r = 0; r < pha.radix_partitions; r++) {
				sel_vectors[r].Initialize();
				sel_vector_sizes[r] = 0;
			}
			assert(hashes.vector_type == VectorType::FLAT_VECTOR);
			auto hashes_ptr = FlatVector::GetData<hash_t>(hashes);
			for (idx_t i = 0; i < groups.size(); i++) {
				auto partition = (hashes_ptr[i] & radix_mask) >> 32;
				assert(partition < pha.radix_partitions);
				sel_vectors[partition].set_index(sel_vector_sizes[partition]++, i);
			}

#ifdef DEBUG
			// make sure we have lost no rows
			idx_t total_count = 0;
			for (idx_t r = 0; r < pha.radix_partitions; r++) {
				total_count += sel_vector_sizes[r];
			}
			assert(total_count == groups.size());
#endif

			DataChunk group_subset, payload_subset;
			auto group_types = groups.GetTypes();
			auto payload_types = payload.GetTypes();
			group_subset.Initialize(group_types);
			payload_subset.Initialize(payload_types);

			idx_t new_groups = 0;
			for (hash_t r = 0; r < pha.radix_partitions; r++) {
				Vector hashes_subset(LogicalType::HASH);

				group_subset.Slice(groups, sel_vectors[r], sel_vector_sizes[r]);
				payload_subset.Slice(payload, sel_vectors[r], sel_vector_sizes[r]);
				hashes_subset.Slice(hashes, sel_vectors[r], sel_vector_sizes[r]);

				new_groups += radix_partitioned_hts[r].AddChunk(*this, group_subset, hashes_subset, payload_subset);
			}
			return new_groups;
		} else {
			return unpartitioned_ht->AddChunk(groups, hashes, payload);
		}
	}

	void Partition() {
		if (pha.radix_partitions < 2) {
			return;
		}
		if (is_partitioned) {
			return;
		}
		unordered_map<hash_t, GroupedAggregateHashTable *> partition_hts;
		assert(radix_partitioned_hts.size() == 0);
		for (idx_t r = 0; r < pha.radix_partitions; r++) {
			hash_t radix = r << 32;
			radix_partitioned_hts[r].NewHT(*this);
			partition_hts[radix] = radix_partitioned_hts[r].hts.back().get();
			assert((radix & radix_mask) == radix);
		}

		unpartitioned_ht->Partition(partition_hts);
		unpartitioned_ht.reset();
		is_partitioned = true;
	}
};

void HashTableList::NewHT(PartitionableHashTable &pht) {
	hts.push_back(make_unique<GroupedAggregateHashTable>(pht.pha.buffer_manager, pht.pha.group_types,
	                                                     pht.pha.payload_types, pht.pha.bindings,
	                                                     HtEntryType::HT_WIDTH_64));
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions, PhysicalOperatorType type)
    : PhysicalHashAggregate(context, types, move(expressions), {}, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, PhysicalOperatorType type)
    : PhysicalSink(type, types), groups(move(groups_p)), all_combinable(true), any_distinct(false),
      buffer_manager(BufferManager::GetBufferManager(context)), radix_partitions(1) {
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

	// TODO, figure out some heuristic/constant for those
	ht_load_limit = 100000;
	radix_limit = 100;

	auto _n_partitions = (idx_t)TaskScheduler::GetScheduler(context).NumberOfThreads();
	while (radix_partitions <= _n_partitions / 2) {
		radix_partitions *= 2;
		if (radix_partitions >= 256) {
			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalOperatorState {
public:
	HashAggregateGlobalState(PhysicalHashAggregate &_op) : op(_op), is_empty(true), lossy_total_groups(0) {
	}

	PhysicalHashAggregate &op;

	vector<unique_ptr<PartitionableHashTable>> partitioned_hts;
	vector<unique_ptr<GroupedAggregateHashTable>> finalized_hts;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
	//! The lock for updating the global aggregate state
	std::mutex lock;
	//    HashTableList ht; ??
	idx_t lossy_total_groups;

	idx_t radix_partitions;
};

class HashAggregateLocalState : public LocalSinkState {
public:
	HashAggregateLocalState(PhysicalHashAggregate &_op) : op(_op), group_executor(op.groups), is_empty(true) {
		ht = make_unique<PartitionableHashTable>(op);
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
	return make_unique<HashAggregateGlobalState>(*this);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<HashAggregateLocalState>(*this);
}

unique_ptr<GroupedAggregateHashTable> PhysicalHashAggregate::NewHT(LocalSinkState &lstate, HtEntryType entry_type) {
	return make_unique<GroupedAggregateHashTable>(buffer_manager, group_types, payload_types, bindings, entry_type);
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
		assert(0); // FIXME
		           //		if (gstate.hts[0].empty()) {
		           //			gstate.hts[0].push_back(NewHT(lstate));
		           //		}
		           //		assert(gstate.hts[0].size() == 1);
		           //		assert(gstate.hts.size() == 1);
		           //		gstate.lossy_total_groups += gstate.hts[0].back()->AddChunk(group_chunk, hashes, payload_chunk);
		return;
	}

	assert(all_combinable);
	assert(!any_distinct);

	if (group_chunk.size() > 0) {
		llstate.is_empty = false;
	}

	if (radix_partitions > 1 && gstate.lossy_total_groups > radix_limit) {
		llstate.ht->Partition();
	}

	gstate.lossy_total_groups += llstate.ht->AddChunk(group_chunk, payload_chunk);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalHashAggregateState : public PhysicalOperatorState {
public:
	PhysicalHashAggregateState(PhysicalOperator &op, vector<LogicalType> &group_types,
	                           vector<LogicalType> &aggregate_types, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), ht_index(0), ht_scan_position(0) {
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
	idx_t ht_index;
	idx_t ht_scan_position;
};

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalState &)lstate;

	// this actually does not do a lot but just pushes the local HTs into the global state so we can later combine them
	// in parallel

	if (!all_combinable) {
		assert(0); // FIXME
		           //		assert(source.hts.size() == 0);
		           //		assert(gstate.hts.size() <= 1);
		           //		assert(gstate.hts.size() == 1 && gstate.hts[0].size() == 1);
		return;
	}

	if (radix_partitions > 1 && gstate.lossy_total_groups > radix_limit) {
		llstate.ht->Partition();
	}

	lock_guard<mutex> glock(gstate.lock);
	assert(all_combinable);
	if (!llstate.is_empty) {
		gstate.is_empty = false;
	}

	// at this point we just collect them the PhysicalHashAggregateFinalizeTask (below) will merge them in parallel
	gstate.partitioned_hts.push_back(move(llstate.ht));
}

// this task is run in multiple threads and combines the radix-partitioned hash tables into a single onen and then
// folds them into the global ht finally.
class PhysicalHashAggregateFinalizeTask : public Task {
public:
	PhysicalHashAggregateFinalizeTask(Pipeline &parent_, HashAggregateGlobalState &state_, idx_t radix_)
	    : parent(parent_), state(state_), radix(radix_) {
	}
	static void FinalizeHT(HashAggregateGlobalState &gstate, idx_t radix) {

		printf("FinalizeHT(%llu)\n", radix);
		//		assert (gstate.partitioned_hts.size() > 1); // TODO does this always hold
		assert(gstate.finalized_hts[radix]);

		for (auto &pht : gstate.partitioned_hts) {
			assert(pht->radix_partitioned_hts.size() == gstate.op.radix_partitions);
			for (auto &ht : pht->radix_partitioned_hts[radix].hts) {
				printf("\t%llu %llu\n", radix, ht->Size());
				// TODO optimization: copy payload from first ht into result since result is empty at this point, just
				// need to rehash
				gstate.finalized_hts[radix]->Combine(*ht);
				// ht.reset();
			}
		}
		// pht.reset();
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

	this->sink_state = move(state);
	auto &gstate = (HashAggregateGlobalState &)*this->sink_state;

	// special case if we have non-combinable aggregates
	if (!all_combinable) {
		// TODO fixme
		//		assert(gstate.hts.size() <= 1);
		//		assert(gstate.hts.size() == 1 && gstate->hts[0].size() == 1);
	}

	// go through all of the child hts and see if we ever called partition() on any of them
	bool any_partitioned = false;
	for (auto &pht : gstate.partitioned_hts) {
		if (pht->is_partitioned) {
			any_partitioned = true;
			break;
		}
	}

	// if one is partitioned, all have to be
	// this should mostly have already happened in Combine, but if not we do it here
	if (any_partitioned) {
		for (auto &pht : gstate.partitioned_hts) {
			pht->Partition();
		}
		// schedule additional tasks to combine the partial HTs
		pipeline.total_tasks += radix_partitions;
		gstate.finalized_hts.resize(radix_partitions);
		for (idx_t r = 0; r < radix_partitions; r++) {
			// TODO possible optimization, if total count < limit for 32 bit ht, use that one
			// create this ht here so finalize needs no lock on gstate
			gstate.finalized_hts[r] = make_unique<GroupedAggregateHashTable>(buffer_manager, group_types, payload_types,
			                                                                 bindings, HtEntryType::HT_WIDTH_64);
			auto t = make_unique<PhysicalHashAggregateFinalizeTask>(pipeline, gstate, r);
			TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(t));
		}
	} else {
		assert(0); // FIXME
	}
}

// fuck it
void PhysicalHashAggregate::FinalizeImmediate(ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	assert(0); // FIXME
	//	this->sink_state = move(state);
	//	auto gstate = (HashAggregateGlobalState *)this->sink_state.get();
	//
	//	for (auto &ht_entry : gstate->hts) {
	//		PhysicalHashAggregateFinalizeTask::FinalizeHT(*gstate, ht_entry.first);
	//	}
}

void PhysicalHashAggregate::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                             PhysicalOperatorState *state_) {
	auto &gstate = (HashAggregateGlobalState &)*sink_state;
	auto &state = (PhysicalHashAggregateState &)*state_;

	state.group_chunk.Reset();
	state.aggregate_chunk.Reset();

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
		//		if (gstate.finalized_hts[state.ht_index].size() == 0) {
		//			// nothing in hash tables since no data was scanned
		//			state.ht_index++;
		//			state.ht_scan_position = 0;
		//			continue;
		//		}
		//		assert(gstate.finalized_hts[state.ht_index].size() == 1);
		elements_found = gstate.finalized_hts[state.ht_index]->Scan(state.ht_scan_position, state.group_chunk,
		                                                            state.aggregate_chunk);

		if (elements_found > 0) {
			break;
		}
		state.ht_index++;
		state.ht_scan_position = 0;
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
