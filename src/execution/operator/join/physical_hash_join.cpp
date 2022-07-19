#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   const vector<idx_t> &left_projection_map,
                                   const vector<idx_t> &right_projection_map_p, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_stats)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(move(delim_types)),
      perfect_join_statistics(move(perfect_join_stats)) {

	children.push_back(move(left));
	children.push_back(move(right));

	D_ASSERT(left_projection_map.empty());
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}

	// for ANTI, SEMI and MARK join, we only need to store the keys, so for these the build types are empty
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI && join_type != JoinType::MARK) {
		build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
	}
}

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_state)
    : PhysicalHashJoin(op, move(left), move(right), move(cond), join_type, {}, {}, {}, estimated_cardinality,
                       std::move(perfect_join_state)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashJoinGlobalSinkState : public GlobalSinkState {
public:
	HashJoinGlobalSinkState() : finalized(false) {
	}

	//! Global HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized = false;

	//! Whether we are doing an external join
	bool external;
	//! Memory usage per thread during the Sink and Execute phases
	idx_t max_ht_size;
	idx_t execute_memory_per_thread;

	//! Hash tables built by each thread
	mutex local_ht_lock;
	vector<unique_ptr<JoinHashTable>> local_hash_tables;
};

class HashJoinLocalSinkState : public LocalSinkState {
public:
	HashJoinLocalSinkState(Allocator &allocator, const PhysicalHashJoin &hj) : build_executor(allocator) {
		if (!hj.right_projection_map.empty()) {
			build_chunk.Initialize(allocator, hj.build_types);
		}
		for (auto &cond : hj.conditions) {
			build_executor.AddExpression(*cond.right);
		}
		join_keys.Initialize(allocator, hj.condition_types);
	}

public:
	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;

	//! Thread-local local HT
	unique_ptr<JoinHashTable> hash_table;
};

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<HashJoinGlobalSinkState>();
	state->hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions, build_types, join_type);
	if (!delim_types.empty() && join_type == JoinType::MARK) {
		// correlated MARK join
		if (delim_types.size() + 1 == conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = state->hash_table->correlated_mark_join_info;

			vector<LogicalType> payload_types;
			vector<BoundAggregateExpression *> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs
			aggr = AggregateFunction::BindAggregateFunction(context, CountStarFun::GetFunction(), {}, nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto count_fun = CountFun::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_unique_base<Expression, BoundReferenceExpression>(count_fun.return_type, 0));
			aggr = AggregateFunction::BindAggregateFunction(context, count_fun, move(children), nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto &allocator = Allocator::Get(context);
			info.correlated_counts = make_unique<GroupedAggregateHashTable>(
			    allocator, BufferManager::GetBufferManager(context), delim_types, payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(allocator, delim_types);
			info.result_chunk.Initialize(allocator, payload_types);
		}
	}
	// for perfect hash join
	state->perfect_join_executor =
	    make_unique<PerfectHashJoinExecutor>(*this, *state->hash_table, perfect_join_statistics);
	// for external hash join
	state->external = ClientConfig::GetConfig(context).force_external;
	// memory usage per thread scales with max mem / num threads
	double max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	double num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	// during the Sink phase we spill at 60%
	state->max_ht_size = max_memory / num_threads * 0.6;
	// during the Execute phase we are probing and sinking at the same time
	// at this point we're already doing an external join, and we need to spill the new sink data
	// we can spill at a much lower percentage, 10%
	state->execute_memory_per_thread = max_memory / num_threads * 0.1;
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto state = make_unique<HashJoinLocalSinkState>(allocator, *this);
	return move(state);
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                      DataChunk &input) const {
	auto &gstate = (HashJoinGlobalSinkState &)gstate_p;
	auto &lstate = (HashJoinLocalSinkState &)lstate_p;
	if (!lstate.hash_table) {
		lstate.hash_table = gstate.hash_table->CopyEmpty();
	}
	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.build_executor.Execute(input, lstate.join_keys);
	// TODO: add statement to check for possible per
	// build the HT
	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
		lstate.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	} else if (!build_types.empty()) {
		// there is not a projected map: place the entire right chunk in the HT
		lstate.hash_table->Build(lstate.join_keys, input);
	} else {
		// there are only keys: place an empty chunk in the payload
		lstate.build_chunk.SetCardinality(input.size());
		lstate.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	}

	// swizzle if we reach memory limit
	if (lstate.hash_table->SizeInBytes() >= gstate.max_ht_size) {
		lstate.hash_table->SwizzleBlocks();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (HashJoinGlobalSinkState &)gstate_p;
	auto &lstate = (HashJoinLocalSinkState &)lstate_p;
	if (lstate.hash_table) {
		lock_guard<mutex> local_ht_lock(gstate.local_ht_lock);
		gstate.local_hash_tables.push_back(move(lstate.hash_table));
	}
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &lstate.build_executor, "build_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	auto &sink = (HashJoinGlobalSinkState &)gstate;

	if (sink.external) {
		// External join - partition HT
		sink.perfect_join_executor.reset();
		sink.hash_table->SetTuplesPerPartitionedProbe(sink.local_hash_tables, sink.max_ht_size);
		sink.hash_table->SchedulePartitionTasks(pipeline, event, sink.local_hash_tables);
		sink.finalized = true;
		return SinkFinalizeType::READY;
	} else {
		for (auto &local_ht : sink.local_hash_tables) {
			sink.hash_table->Merge(*local_ht);
		}
		sink.local_hash_tables.clear();
	}

	// check for possible perfect hash table
	auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
	if (use_perfect_hash) {
		D_ASSERT(sink.hash_table->equality_types.size() == 1);
		auto key_type = sink.hash_table->equality_types[0];
		use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
	}
	// In case of a large build side or duplicates, use regular hash join
	if (!use_perfect_hash) {
		sink.perfect_join_executor.reset();
		sink.hash_table->Finalize();
	}
	sink.finalized = true;
	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PhysicalHashJoinState : public OperatorState {
public:
	explicit PhysicalHashJoinState(Allocator &allocator) : probe_executor(allocator), local_sink_ht(nullptr) {
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

	//! DataChunks for sinking data into local_sink_ht for external join
	DataChunk sink_keys;
	DataChunk sink_payload;

	//! Local ht to sink data into for external join
	JoinHashTable *local_sink_ht;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &probe_executor, "probe_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	auto state = make_unique<PhysicalHashJoinState>(allocator);
	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	} else {
		state->join_keys.Initialize(allocator, condition_types);
		for (auto &cond : conditions) {
			state->probe_executor.AddExpression(*cond.left);
		}
	}
	if (sink.external) {
		const auto &payload_types = children[0]->types;
		state->sink_keys.Initialize(allocator, condition_types);
		state->sink_payload.Initialize(allocator, payload_types);

		lock_guard<mutex> local_ht_lock(sink.local_ht_lock);
		sink.local_hash_tables.push_back(
		    make_unique<JoinHashTable>(sink.hash_table->buffer_manager, conditions, payload_types, join_type));
		state->local_sink_ht = sink.local_hash_tables.back().get();
	}

	return move(state);
}

OperatorResultType PhysicalHashJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (PhysicalHashJoinState &)state_p;
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	D_ASSERT(sink.finalized);

	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return OperatorResultType::FINISHED;
	}

	if (sink.perfect_join_executor) {
		D_ASSERT(!sink.external);
		return sink.perfect_join_executor->ProbePerfectHashTable(context, input, chunk, *state.perfect_hash_join_state);
	}

	if (state.scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got >1024 elements in the previous probe)
		state.scan_structure->Next(state.join_keys, input, chunk);
		if (chunk.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.scan_structure = nullptr;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// probe the HT
	if (sink.hash_table->Count() == 0) {
		ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, input, chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// resolve the join keys for the left chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);

	// perform the actual probe
	if (sink.external) {
		if (state.local_sink_ht->SizeInBytes() >= sink.execute_memory_per_thread) {
			// Swizzle data collected so far (if needed)
			state.local_sink_ht->SwizzleBlocks();
		}
		state.scan_structure = sink.hash_table->ProbeAndBuild(state.join_keys, input, *state.local_sink_ht,
		                                                      state.sink_keys, state.sink_payload);
	} else {
		state.scan_structure = sink.hash_table->Probe(state.join_keys);
	}
	state.scan_structure->Next(state.join_keys, input, chunk);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashJoinGlobalSourceState : public GlobalSourceState {
public:
	HashJoinGlobalSourceState() : probe_count(0) {
	}

	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState full_outer_scan_state;

	//! Only used for external join: Materialized probe-side data and scan structure
	unique_ptr<JoinHashTable> probe_ht;
	JoinHTScanState probe_scan_state;

	idx_t probe_count;

	idx_t MaxThreads() override {
		return probe_count / ((idx_t)STANDARD_VECTOR_SIZE * 10);
	}
};

//! Only used for external join
class HashJoinLocalSourceState : public LocalSourceState {
public:
	HashJoinLocalSourceState() : addresses(LogicalType::POINTER) {
	}

	//! Same as the input chunk in PhysicalHashJoin::Execute
	DataChunk join_keys;
	DataChunk payload;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;

	Vector addresses;
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;

	auto result = make_unique<HashJoinGlobalSourceState>();
	if (sink.external) {
		result->probe_ht =
		    make_unique<JoinHashTable>(sink.hash_table->buffer_manager, conditions, children[0]->types, join_type);
	}

	for (auto &local_ht : sink.local_hash_tables) {
		result->probe_count += local_ht->Count() + local_ht->SwizzledCount();
	}

	return result;
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	auto &allocator = Allocator::Get(context.client);
	auto result = make_unique<HashJoinLocalSourceState>();
	result->join_keys.Initialize(allocator, condition_types);
	result->payload.Initialize(allocator, children[0]->types);
	// TODO: maybe assert that the chunk types match the ht types
	return result;
}

bool PhysicalHashJoin::PrepareProbeRound(HashJoinGlobalSourceState &gstate) const {
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;

	// Prepare the build side
	sink.hash_table->FinalizeExternal();
	if (!sink.hash_table->finalized) {
		// Done
		return false;
	}
	// Prepare the probe side
	gstate.probe_ht->PreparePartitionedProbe(*sink.hash_table, gstate.probe_scan_state);
	// Reset full outer scan state (if necessary)
	if (IsRightOuterJoin(join_type)) {
		gstate.full_outer_scan_state.Reset();
	}
	return true;
}

//! Probe the build side using the materialized and partitioned probe side
void ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, HashJoinLocalSourceState &lstate,
                   JoinHTScanState &probe_ss, DataChunk &chunk) {
	// Grab the next read position
	idx_t position;
	idx_t block_position;
	idx_t count = gstate.probe_ht->GetScanIndices(probe_ss, position, block_position);

	if (count == 0) {
		// Done!
		return;
	}

	// Construct input Chunk for next probe
	gstate.probe_ht->ConstructProbeChunk(lstate.join_keys, lstate.payload, lstate.addresses, position, block_position,
	                                     count);

	// Perform the probe
	// TODO optimization: we already have the hashes
	lstate.scan_structure = sink.hash_table->Probe(lstate.join_keys);
	lstate.scan_structure->Next(lstate.join_keys, lstate.payload, chunk);
}

void PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                               LocalSourceState &lstate_p) const {
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	auto &gstate = (HashJoinGlobalSourceState &)gstate_p;
	auto &lstate = (HashJoinLocalSourceState &)lstate_p;

	if (!sink.external) {
		if (IsRightOuterJoin(join_type)) {
			// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
			idx_t found_entries;
			{
				lock_guard<mutex> fo_lock(gstate.full_outer_scan_state.lock);
				found_entries = sink.hash_table->ScanFullOuter(gstate.full_outer_scan_state, lstate.addresses);
			}
			sink.hash_table->GatherFullOuter(chunk, lstate.addresses, found_entries);
		}
		return;
	}

	// This is an external join
	D_ASSERT(gstate.probe_ht);

	// Partition thread-local probe hts
	while (!sink.local_hash_tables.empty()) {
		unique_ptr<JoinHashTable> local_ht;
		// Check if anything has changed after we grab the lock
		if (!sink.local_hash_tables.empty()) {
			lock_guard<mutex> local_ht_lock(sink.local_ht_lock);

			// TODO have to put this here for now, doesn't work if it's in GetGlobalSourceState
			gstate.probe_ht->radix_bits = sink.hash_table->radix_bits;

			local_ht = move(sink.local_hash_tables.back());
			sink.local_hash_tables.pop_back();
		}
		local_ht->Partition(*gstate.probe_ht);
	}

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done, in one of three ways:
	// 1. Get remaining elements from previous probe
	// 2. Probe
	// 3. Scanning HT for OUTER joins
	// Probing is done in phases: This loop also makes sure that threads wait while a thread prepares the next phase
	while (chunk.size() == 0) {
		auto &probe_ss = gstate.probe_scan_state;
		if (lstate.scan_structure) {
			// Still have elements remaining from the previous probe (i.e. we got >1024 elements in the previous probe)
			lstate.scan_structure->Next(lstate.join_keys, lstate.payload, chunk);
			if (chunk.size() == 0) {
				// Probe done
				lstate.scan_structure = nullptr;
				probe_ss.scanned += lstate.join_keys.size();
				D_ASSERT(probe_ss.scanned <= probe_ss.total);
			}
		} else if (probe_ss.scan_index < probe_ss.total) {
			ExternalProbe(sink, gstate, lstate, probe_ss, chunk);
		} else if (probe_ss.scan_index == probe_ss.total) {
			// We are done probing this partition
			idx_t found_entries = 0;
			do {
				// Scan full outer (if necessary)
				lock_guard<mutex> fo_lock(gstate.full_outer_scan_state.lock);
				if (IsRightOuterJoin(join_type)) {
					found_entries = sink.hash_table->ScanFullOuter(gstate.full_outer_scan_state, lstate.addresses);
					if (found_entries != 0) {
						break;
					}
				}
				// We hold the full outer lock and know that it is done
				// Grab the probe lock and see if anything has changed
				lock_guard<mutex> probe_lock(probe_ss.lock);
				if (probe_ss.scan_index != probe_ss.total) {
					// Another thread has prepared the next partitioned probe
					break;
				}
				// Try to prepare the next partitioned probe
				if (!PrepareProbeRound(gstate)) {
					// All partitions done
					return;
				}
				break;
			} while (true);
			// If we found entries from the full outer join, gather them
			sink.hash_table->GatherFullOuter(chunk, lstate.addresses, found_entries);
		}
	}
}

} // namespace duckdb
