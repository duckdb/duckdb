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
class HashJoinGlobalState : public GlobalSinkState {
public:
	HashJoinGlobalState() : finalized(false) {
	}

	//! Global HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized = false;

	//! Whether we are doing an external join
	bool external;
	//! TODO
	//! Memory usage per thread during the Sink and Execute phases
	idx_t sink_memory_per_thread;
	idx_t execute_memory_per_thread;

	//! Hash tables built by each thread
	mutex local_ht_lock;
	vector<unique_ptr<JoinHashTable>> local_hash_tables;
};

class HashJoinLocalState : public LocalSinkState {
public:
	//! Thread-local local HT
	unique_ptr<JoinHashTable> hash_table;
	//! Whether the hash_table has been initialized
	bool initialized;

	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;

	void Initialize(const HashJoinGlobalState &gstate) {
		hash_table = gstate.hash_table->CopyEmpty();
		initialized = true;
	}
};

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<HashJoinGlobalState>();
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

			info.correlated_counts = make_unique<GroupedAggregateHashTable>(
			    BufferManager::GetBufferManager(context), delim_types, payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(delim_types);
			info.result_chunk.Initialize(payload_types);
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
	state->sink_memory_per_thread = max_memory / num_threads * 0.6;
	// during the Execute phase we are probing and sinking at the same time
	// at this point we're already doing an external join, and we need to spill the new sink data
	// we can spill at a much lower percentage, like 10%
	state->execute_memory_per_thread = max_memory / num_threads * 0.6;
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_unique<HashJoinLocalState>();
	state->initialized = false;
	if (!right_projection_map.empty()) {
		state->build_chunk.Initialize(build_types);
	}
	for (auto &cond : conditions) {
		state->build_executor.AddExpression(*cond.right);
	}
	state->join_keys.Initialize(condition_types);
	return move(state);
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                      DataChunk &input) const {
	auto &gstate = (HashJoinGlobalState &)gstate_p;
	auto &lstate = (HashJoinLocalState &)lstate_p;
	if (!lstate.initialized) {
		lstate.Initialize(gstate);
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
	if (lstate.hash_table->SizeInBytes() >= gstate.sink_memory_per_thread) {
		lstate.hash_table->SwizzleCollectedBlocks();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (HashJoinGlobalState &)gstate_p;
	auto &lstate = (HashJoinLocalState &)lstate_p;
	if (lstate.initialized) {
		if (gstate.external) {
			lstate.hash_table->SwizzleCollectedBlocks();
		}
		gstate.local_hash_tables.push_back(move(lstate.hash_table));
		// TODO some hash tables may be swizzled while others aren't due to race conditions
		//  need to ensure all of them are swizzled!
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
	auto &sink = (HashJoinGlobalState &)gstate;

	if (sink.external) {
		for (auto &ht : sink.local_hash_tables) {
			if (ht->Count() != 0) {
				// Has unswizzled blocks left
				ht->SwizzleCollectedBlocks();
			}
		}
	} else {
		// Merge local hash tables into the main hash table
		for (auto &ht : sink.local_hash_tables) {
			sink.hash_table->Merge(*ht);
		}
	}

	// check for possible perfect hash table (can't if this is an external join)
	auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin() && !sink.external;
	if (use_perfect_hash) {
		D_ASSERT(sink.hash_table->equality_types.size() == 1);
		auto key_type = sink.hash_table->equality_types[0];
		use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
	}
	// In case of a large build side or duplicates, use regular hash join
	if (!use_perfect_hash) {
		sink.perfect_join_executor.reset();
		if (sink.external) {
			sink.hash_table->SchedulePartitionTasks(pipeline, event, sink.local_hash_tables);
		} else {
			sink.hash_table->Finalize();
			if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
				return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
			}
		}
	}
	sink.finalized = true;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PhysicalHashJoinState : public OperatorState {
public:
	PhysicalHashJoinState() : local_ht(nullptr) {
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

	//! DataChunks for sinking data into local_sink for external join
	DataChunk sink_keys;
	DataChunk sink_payload;
	//! Local ht to sink data into for external join
	JoinHashTable *local_ht;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &probe_executor, "probe_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ClientContext &context) const {
	auto state = make_unique<PhysicalHashJoinState>();
	auto &sink = (HashJoinGlobalState &)*sink_state;
	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	} else {
		state->join_keys.Initialize(condition_types);
		for (auto &cond : conditions) {
			state->probe_executor.AddExpression(*cond.left);
		}
	}
	if (sink.external) {
		state->sink_keys.Initialize(condition_types);
		state->sink_payload.Initialize(children[0]->types);
		lock_guard<mutex> local_ht_lock(sink.local_ht_lock);
		sink.local_hash_tables.push_back(
		    make_unique<JoinHashTable>(sink.hash_table->buffer_manager, conditions, types, join_type));
		state->local_ht = sink.local_hash_tables.back().get();
	}

	return move(state);
}

OperatorResultType PhysicalHashJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (PhysicalHashJoinState &)state_p;
	auto &sink = (HashJoinGlobalState &)*sink_state;
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
		state.scan_structure = sink.hash_table->ProbeAndBuild(state.join_keys, input, *state.local_ht, state.sink_keys,
		                                                      state.sink_payload);
		if (state.local_ht->SizeInBytes() >= sink.execute_memory_per_thread) {
			state.local_ht->SwizzleCollectedBlocks();
		}
	} else {
		state.scan_structure = sink.hash_table->Probe(state.join_keys);
	}
	state.scan_structure->Next(state.join_keys, input, chunk);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashJoinGlobalScanState : public GlobalSourceState {
public:
	explicit HashJoinGlobalScanState(const PhysicalHashJoin &op) : op(op) {
		auto &sink = (HashJoinGlobalState &)*op.sink_state;
		if (sink.external) {
			probe_ht = sink.hash_table->CopyEmpty();
			sink.hash_table->finalized = false; // TODO check if this is OK
		}
	}

	const PhysicalHashJoin &op;
	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState full_outer_scan_state;

	//! Only used for external join: Materialized probe-side data and scan structure
	unique_ptr<JoinHashTable> probe_ht;
	JoinHTScanState probe_scan_state;

	idx_t MaxThreads() override {
		auto &sink = (HashJoinGlobalState &)*op.sink_state;
		return sink.hash_table->Count() / ((idx_t)STANDARD_VECTOR_SIZE * 10);
	}
};

class HashJoinLocalScanState : public LocalSourceState {
public:
	explicit HashJoinLocalScanState(const PhysicalHashJoin &op) : partitioned(false), addresses(LogicalType::POINTER) {
		// TODO: oops ... the build types of probe and build side are not the same!
		probe_chunk.Initialize(op.build_types);
		join_keys.Initialize(op.condition_types);
		payload.Initialize(op.children[0]->types);
	}

	//! Whether this thread has partitioned its local HT yet
	bool partitioned;

	//! TODO
	Vector addresses;

	//! Probe keys and payload TODO
	DataChunk probe_chunk;
	DataChunk join_keys;
	DataChunk payload;

	//! TODO
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<HashJoinGlobalScanState>(*this);
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_unique<HashJoinLocalScanState>(*this);
}

void PhysicalHashJoin::PrepareProbeRound(HashJoinGlobalScanState &gstate) const {
	auto &sink = (HashJoinGlobalState &)*sink_state;
	// Prepare the build side
	sink.hash_table->FinalizeExternal();
	// Prepare the probe side
	gstate.probe_ht->PreparePartitionedProbe(*sink.hash_table, gstate.probe_scan_state);
	// Reset full outer scan state (if necessary)
	if (IsRightOuterJoin(join_type)) {
		auto &outer_ss = gstate.full_outer_scan_state;
		lock_guard<mutex> fo_lock(outer_ss.lock);
		outer_ss.Reset();
	}
	// TODO: also pin the probe stuffff
}

void PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                               LocalSourceState &lstate_p) const {
	auto &sink = (HashJoinGlobalState &)*sink_state;
	auto &gstate = (HashJoinGlobalScanState &)gstate_p;
	auto &lstate = (HashJoinLocalScanState &)lstate_p;

	if (!sink.external) {
		D_ASSERT(IsRightOuterJoin(join_type));
		// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
		sink.hash_table->ScanFullOuter(chunk, gstate.full_outer_scan_state, lstate.addresses);
		return;
	}

	// This is an external join
	if (!lstate.partitioned) {
		// Partition thread-local probe HT
		auto local_ht = AssignLocalHT(sink);
		local_ht->Partition(*gstate.probe_ht);
	}

	auto &probe_ss = gstate.probe_scan_state;
	{
		lock_guard<mutex> lock(probe_ss.lock);
		if (sink.finalized && probe_ss.finished) {
			// Partitioned probe is done TODO maybe change this, see below
			lock_guard<mutex> flock(sink.finalize_lock);
			sink.finalized = false;
		}
	}

	if (!sink.finalized) {
		if (IsRightOuterJoin(join_type)) {
			// We scan the previous partition's unmatched tuples (if outer join)
			sink.hash_table->ScanFullOuter(chunk, gstate.full_outer_scan_state, lstate.addresses);
			if (chunk.size() > 0) {
				return;
			}
		}

		lock_guard<mutex> flock(sink.finalize_lock);
		// Check again, maybe it was finalized while we waited for the lock
		if (!sink.finalized) {
			PrepareProbeRound(gstate);
		}
	}
	D_ASSERT(sink.finalized);

	// Both build and probe sides are materialized and partitioned, and the next partitioned pointer table is built
	if (lstate.scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got >1024 elements in the previous probe)
		lstate.scan_structure->Next(lstate.join_keys, lstate.input, chunk);
		if (chunk.size() > 0) {
			return;
		}
		lstate.scan_structure = nullptr;
		// TODO: update probe_ss progress
		//  should spinlock other threads while we can't increment scan position, but aren't done scanning yet
	}

	// Grab the next read position
	idx_t probe_chunk_count;
	idx_t position;
	idx_t block_position;
	{
		lock_guard<mutex> lock(probe_ss.lock);
		if (probe_ss.Finished()) {
			// TODO: construct next partition
		}
		probe_chunk_count = gstate.probe_ht->GetScanIndices(probe_ss, position, block_position);
	}

	if (probe_chunk_count == 0) {
		// TODO what?
	}

	// Construct input Chunk for next probe
	gstate.probe_ht->ConstructProbeChunk(lstate.input, lstate.addresses, position, block_position, probe_chunk_count);

	// Grab the join keys
	for (idx_t i = 0; i < lstate.join_keys.ColumnCount(); i++) {
		lstate.join_keys.data[i].Reference(lstate.input.data[i]);
	}
	lstate.join_keys.SetCardinality(probe_chunk_count);

	// Perform the probe
	// TODO optimization: we already have the hashes
	lstate.scan_structure = sink.hash_table->Probe(lstate.join_keys);
	lstate.scan_structure->Next(lstate.join_keys, lstate.input, chunk);
}

} // namespace duckdb
