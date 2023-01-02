#include "duckdb/execution/radix_partitioned_hashtable.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/event.hpp"

namespace duckdb {

// compute the GROUPING values
// for each parameter to the GROUPING clause, we check if the hash table groups on this particular group
// if it does, we return 0, otherwise we return 1
// we then use bitshifts to combine these values
void RadixPartitionedHashTable::SetGroupingValues() {
	auto &grouping_functions = op.GetGroupingFunctions();
	for (auto &grouping : grouping_functions) {
		int64_t grouping_value = 0;
		D_ASSERT(grouping.size() < sizeof(int64_t) * 8);
		for (idx_t i = 0; i < grouping.size(); i++) {
			if (grouping_set.find(grouping[i]) == grouping_set.end()) {
				// we don't group on this value!
				grouping_value += (int64_t)1 << (grouping.size() - (i + 1));
			}
		}
		grouping_values.push_back(Value::BIGINT(grouping_value));
	}
}

RadixPartitionedHashTable::RadixPartitionedHashTable(GroupingSet &grouping_set_p, const GroupedAggregateData &op_p)
    : grouping_set(grouping_set_p), op(op_p) {

	auto groups_count = op.GroupCount();
	for (idx_t i = 0; i < groups_count; i++) {
		if (grouping_set.find(i) == grouping_set.end()) {
			null_groups.push_back(i);
		}
	}

	// 10000 seems like a good compromise here
	radix_limit = 10000;

	if (grouping_set.empty()) {
		// fake a single group with a constant value for aggregation without groups
		group_types.emplace_back(LogicalType::TINYINT);
	}
	for (auto &entry : grouping_set) {
		D_ASSERT(entry < op.group_types.size());
		group_types.push_back(op.group_types[entry]);
	}
	SetGroupingValues();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RadixHTGlobalState : public GlobalSinkState {
public:
	explicit RadixHTGlobalState(ClientContext &context)
	    : is_empty(true), multi_scan(true), total_groups(0),
	      partition_info((idx_t)TaskScheduler::GetScheduler(context).NumberOfThreads()) {
	}

	vector<unique_ptr<PartitionableHashTable>> intermediate_hts;
	vector<shared_ptr<GroupedAggregateHashTable>> finalized_hts;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
	//! Whether or not the hash table should be scannable multiple times
	bool multi_scan;
	//! The lock for updating the global aggregate state
	mutex lock;
	//! a counter to determine if we should switch over to partitioning
	atomic<idx_t> total_groups;

	bool is_finalized = false;
	bool is_partitioned = false;

	RadixPartitionInfo partition_info;
};

class RadixHTLocalState : public LocalSinkState {
public:
	explicit RadixHTLocalState(const RadixPartitionedHashTable &ht) : is_empty(true) {
		// if there are no groups we create a fake group so everything has the same group
		group_chunk.InitializeEmpty(ht.group_types);
		if (ht.grouping_set.empty()) {
			group_chunk.data[0].Reference(Value::TINYINT(42));
		}
	}

	DataChunk group_chunk;
	//! The aggregate HT
	unique_ptr<PartitionableHashTable> ht;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
};

void RadixPartitionedHashTable::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = (RadixHTGlobalState &)state;
	gstate.multi_scan = true;
}

unique_ptr<GlobalSinkState> RadixPartitionedHashTable::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<RadixHTGlobalState>(context);
}

unique_ptr<LocalSinkState> RadixPartitionedHashTable::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<RadixHTLocalState>(*this);
}

void RadixPartitionedHashTable::PopulateGroupChunk(DataChunk &group_chunk, DataChunk &input_chunk) const {
	idx_t chunk_index = 0;
	// Populate the group_chunk
	for (auto &group_idx : grouping_set) {
		// Retrieve the expression containing the index in the input chunk
		auto &group = op.groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = (BoundReferenceExpression &)*group;
		// Reference from input_chunk[group.index] -> group_chunk[chunk_index]
		group_chunk.data[chunk_index++].Reference(input_chunk.data[bound_ref_expr.index]);
	}
	group_chunk.SetCardinality(input_chunk.size());
	group_chunk.Verify();
}

void RadixPartitionedHashTable::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                     DataChunk &groups_input, DataChunk &payload_input,
                                     const vector<idx_t> &filter) const {
	auto &llstate = (RadixHTLocalState &)lstate;
	auto &gstate = (RadixHTGlobalState &)state;
	D_ASSERT(!gstate.is_finalized);

	DataChunk &group_chunk = llstate.group_chunk;
	PopulateGroupChunk(group_chunk, groups_input);

	// if we have non-combinable aggregates (e.g. string_agg) we cannot keep parallel hash
	// tables
	if (ForceSingleHT(state)) {
		lock_guard<mutex> glock(gstate.lock);
		gstate.is_empty = gstate.is_empty && group_chunk.size() == 0;
		if (gstate.finalized_hts.empty()) {
			// Create a finalized ht in the global state, that we can populate
			gstate.finalized_hts.push_back(
			    make_unique<GroupedAggregateHashTable>(context.client, Allocator::Get(context.client), group_types,
			                                           op.payload_types, op.bindings, HtEntryType::HT_WIDTH_64));
		}
		D_ASSERT(gstate.finalized_hts.size() == 1);
		D_ASSERT(gstate.finalized_hts[0]);
		gstate.total_groups += gstate.finalized_hts[0]->AddChunk(group_chunk, payload_input, filter);
		return;
	}

	if (group_chunk.size() > 0) {
		llstate.is_empty = false;
	}

	if (!llstate.ht) {
		llstate.ht =
		    make_unique<PartitionableHashTable>(context.client, Allocator::Get(context.client), gstate.partition_info,
		                                        group_types, op.payload_types, op.bindings);
	}

	gstate.total_groups +=
	    llstate.ht->AddChunk(group_chunk, payload_input,
	                         gstate.total_groups > radix_limit && gstate.partition_info.n_partitions > 1, filter);
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &state,
                                        LocalSinkState &lstate) const {
	auto &llstate = (RadixHTLocalState &)lstate;
	auto &gstate = (RadixHTGlobalState &)state;
	D_ASSERT(!gstate.is_finalized);

	// this actually does not do a lot but just pushes the local HTs into the global state so we can later combine them
	// in parallel

	if (ForceSingleHT(state)) {
		D_ASSERT(gstate.finalized_hts.size() <= 1);
		return;
	}

	if (!llstate.ht) {
		return; // no data
	}

	if (!llstate.ht->IsPartitioned() && gstate.partition_info.n_partitions > 1 && gstate.total_groups > radix_limit) {
		llstate.ht->Partition();
	}

	lock_guard<mutex> glock(gstate.lock);

	if (!llstate.is_empty) {
		gstate.is_empty = false;
	}

	// we will never add new values to these HTs so we can drop the first part of the HT
	llstate.ht->Finalize();

	// at this point we just collect them the PhysicalHashAggregateFinalizeTask (below) will merge them in parallel
	gstate.intermediate_hts.push_back(std::move(llstate.ht));
}

bool RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = (RadixHTGlobalState &)gstate_p;
	D_ASSERT(!gstate.is_finalized);
	gstate.is_finalized = true;

	// special case if we have non-combinable aggregates
	// we have already aggreagted into a global shared HT that does not require any additional finalization steps
	if (ForceSingleHT(gstate)) {
		D_ASSERT(gstate.finalized_hts.size() <= 1);
		D_ASSERT(gstate.finalized_hts.empty() || gstate.finalized_hts[0]);
		return false;
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

	auto &allocator = Allocator::Get(context);
	if (any_partitioned) {
		// if one is partitioned, all have to be
		// this should mostly have already happened in Combine, but if not we do it here
		for (auto &pht : gstate.intermediate_hts) {
			if (!pht->IsPartitioned()) {
				pht->Partition();
			}
		}
		// schedule additional tasks to combine the partial HTs
		gstate.finalized_hts.resize(gstate.partition_info.n_partitions);
		for (idx_t r = 0; r < gstate.partition_info.n_partitions; r++) {
			gstate.finalized_hts[r] = make_shared<GroupedAggregateHashTable>(
			    context, allocator, group_types, op.payload_types, op.bindings, HtEntryType::HT_WIDTH_64);
		}
		gstate.is_partitioned = true;
		return true;
	} else { // in the non-partitioned case we immediately combine all the unpartitioned hts created by the threads.
		     // TODO possible optimization, if total count < limit for 32 bit ht, use that one
		     // create this ht here so finalize needs no lock on gstate

		gstate.finalized_hts.push_back(make_shared<GroupedAggregateHashTable>(
		    context, allocator, group_types, op.payload_types, op.bindings, HtEntryType::HT_WIDTH_64));
		for (auto &pht : gstate.intermediate_hts) {
			auto unpartitioned = pht->GetUnpartitioned();
			for (auto &unpartitioned_ht : unpartitioned) {
				D_ASSERT(unpartitioned_ht);
				gstate.finalized_hts[0]->Combine(*unpartitioned_ht);
				unpartitioned_ht.reset();
			}
			unpartitioned.clear();
		}
		D_ASSERT(gstate.finalized_hts[0]);
		gstate.finalized_hts[0]->Finalize();
		return false;
	}
}

// this task is run in multiple threads and combines the radix-partitioned hash tables into a single onen and then
// folds them into the global ht finally.
class RadixAggregateFinalizeTask : public ExecutorTask {
public:
	RadixAggregateFinalizeTask(Executor &executor, shared_ptr<Event> event_p, RadixHTGlobalState &state_p,
	                           idx_t radix_p)
	    : ExecutorTask(executor), event(std::move(event_p)), state(state_p), radix(radix_p) {
	}

	static void FinalizeHT(RadixHTGlobalState &gstate, idx_t radix) {
		D_ASSERT(gstate.partition_info.n_partitions <= gstate.finalized_hts.size());
		D_ASSERT(gstate.finalized_hts[radix]);
		for (auto &pht : gstate.intermediate_hts) {
			for (auto &ht : pht->GetPartition(radix)) {
				gstate.finalized_hts[radix]->Combine(*ht);
				ht.reset();
			}
		}
		gstate.finalized_hts[radix]->Finalize();
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		FinalizeHT(state, radix);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	RadixHTGlobalState &state;
	idx_t radix;
};

void RadixPartitionedHashTable::ScheduleTasks(Executor &executor, const shared_ptr<Event> &event,
                                              GlobalSinkState &state, vector<unique_ptr<Task>> &tasks) const {
	auto &gstate = (RadixHTGlobalState &)state;
	if (!gstate.is_partitioned) {
		return;
	}
	for (idx_t r = 0; r < gstate.partition_info.n_partitions; r++) {
		D_ASSERT(gstate.partition_info.n_partitions <= gstate.finalized_hts.size());
		D_ASSERT(gstate.finalized_hts[r]);
		tasks.push_back(make_unique<RadixAggregateFinalizeTask>(executor, event, gstate, r));
	}
}

bool RadixPartitionedHashTable::ForceSingleHT(GlobalSinkState &state) const {
	auto &gstate = (RadixHTGlobalState &)state;
	return gstate.partition_info.n_partitions < 2;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class RadixHTGlobalSourceState : public GlobalSourceState {
public:
	explicit RadixHTGlobalSourceState(Allocator &allocator, const RadixPartitionedHashTable &ht)
	    : ht_index(0), initialized(false), finished(false) {
	}

	//! Heavy handed for now.
	mutex lock;
	//! The current position to scan the HT for output tuples
	idx_t ht_index;
	//! The set of aggregate scan states
	unique_ptr<AggregateHTScanState[]> ht_scan_states;
	atomic<bool> initialized;
	atomic<bool> finished;
};

class RadixHTLocalSourceState : public LocalSourceState {
public:
	explicit RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &ht) {
		auto &allocator = Allocator::Get(context.client);
		auto scan_chunk_types = ht.group_types;
		for (auto &aggr_type : ht.op.aggregate_return_types) {
			scan_chunk_types.push_back(aggr_type);
		}
		scan_chunk.Initialize(allocator, scan_chunk_types);
	}

	//! Materialized GROUP BY expressions & aggregates
	DataChunk scan_chunk;
	//! A reference to the current HT that we are scanning
	shared_ptr<GroupedAggregateHashTable> ht;
};

unique_ptr<GlobalSourceState> RadixPartitionedHashTable::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<RadixHTGlobalSourceState>(Allocator::Get(context), *this);
}

unique_ptr<LocalSourceState> RadixPartitionedHashTable::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<RadixHTLocalSourceState>(context, *this);
}

idx_t RadixPartitionedHashTable::Size(GlobalSinkState &sink_state) const {
	auto &gstate = (RadixHTGlobalState &)sink_state;
	if (gstate.is_empty && grouping_set.empty()) {
		return 1;
	}

	idx_t count = 0;
	for (const auto &ht : gstate.finalized_hts) {
		count += ht->Size();
	}
	return count;
}

void RadixPartitionedHashTable::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSinkState &sink_state,
                                        GlobalSourceState &gsstate, LocalSourceState &lsstate) const {
	auto &gstate = (RadixHTGlobalState &)sink_state;
	auto &state = (RadixHTGlobalSourceState &)gsstate;
	auto &lstate = (RadixHTLocalSourceState &)lsstate;
	D_ASSERT(gstate.is_finalized);
	if (state.finished) {
		return;
	}

	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (gstate.is_empty && grouping_set.empty()) {
		D_ASSERT(chunk.ColumnCount() == null_groups.size() + op.aggregates.size() + op.grouping_functions.size());
		// for each column in the aggregates, set to initial state
		chunk.SetCardinality(1);
		for (auto null_group : null_groups) {
			chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(chunk.data[null_group], true);
		}
		for (idx_t i = 0; i < op.aggregates.size(); i++) {
			D_ASSERT(op.aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*op.aggregates[i];
			auto aggr_state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(aggr_state.get());

			AggregateInputData aggr_input_data(aggr.bind_info.get(), Allocator::DefaultAllocator());
			Vector state_vector(Value::POINTER((uintptr_t)aggr_state.get()));
			aggr.function.finalize(state_vector, aggr_input_data, chunk.data[null_groups.size() + i], 1, 0);
			if (aggr.function.destructor) {
				aggr.function.destructor(state_vector, 1);
			}
		}
		// Place the grouping values (all the groups of the grouping_set condensed into a single value)
		// Behind the null groups + aggregates
		for (idx_t i = 0; i < op.grouping_functions.size(); i++) {
			chunk.data[null_groups.size() + op.aggregates.size() + i].Reference(grouping_values[i]);
		}
		state.finished = true;
		return;
	}
	if (gstate.is_empty) {
		state.finished = true;
		return;
	}
	idx_t elements_found = 0;

	lstate.scan_chunk.Reset();
	lstate.ht.reset();
	if (!state.initialized) {
		lock_guard<mutex> l(state.lock);
		if (!state.ht_scan_states) {
			state.ht_scan_states =
			    unique_ptr<AggregateHTScanState[]>(new AggregateHTScanState[gstate.finalized_hts.size()]);
		} else {
			D_ASSERT(state.initialized);
		}
		state.initialized = true;
	}
	while (true) {
		idx_t ht_index;

		{
			lock_guard<mutex> l(state.lock);
			ht_index = state.ht_index;
			if (ht_index >= gstate.finalized_hts.size()) {
				state.finished = true;
				return;
			}
			D_ASSERT(ht_index < gstate.finalized_hts.size());
			lstate.ht = gstate.finalized_hts[ht_index];
			D_ASSERT(lstate.ht);
		}
		D_ASSERT(state.ht_scan_states);
		auto &scan_state = state.ht_scan_states[ht_index];
		D_ASSERT(lstate.ht);
		elements_found = lstate.ht->Scan(scan_state, lstate.scan_chunk);
		if (elements_found > 0) {
			break;
		}
		// move to the next hash table
		lock_guard<mutex> l(state.lock);
		ht_index++;
		if (ht_index > state.ht_index) {
			// we have not yet worked on the table
			// move the global index forwards
			if (!gstate.multi_scan) {
				gstate.finalized_hts[state.ht_index].reset();
			}
			state.ht_index = ht_index;
		}
	}

	// compute the final projection list
	chunk.SetCardinality(elements_found);

	idx_t chunk_index = 0;
	for (auto &entry : grouping_set) {
		chunk.data[entry].Reference(lstate.scan_chunk.data[chunk_index++]);
	}
	for (auto null_group : null_groups) {
		chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(chunk.data[null_group], true);
	}
	D_ASSERT(grouping_set.size() + null_groups.size() == op.GroupCount());
	for (idx_t col_idx = 0; col_idx < op.aggregates.size(); col_idx++) {
		chunk.data[op.GroupCount() + col_idx].Reference(lstate.scan_chunk.data[group_types.size() + col_idx]);
	}
	D_ASSERT(op.grouping_functions.size() == grouping_values.size());
	for (idx_t i = 0; i < op.grouping_functions.size(); i++) {
		chunk.data[op.GroupCount() + op.aggregates.size() + i].Reference(grouping_values[i]);
	}
}

} // namespace duckdb
