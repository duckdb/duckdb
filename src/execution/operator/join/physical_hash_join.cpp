#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"
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
	idx_t sink_memory_per_thread;

	//! Hash tables built by each thread
	mutex lock;
	vector<unique_ptr<JoinHashTable>> local_hash_tables;

	//! Excess probe data gathered during Sink
	vector<LogicalType> probe_types;
	vector<unique_ptr<ColumnDataCollection>> spill_collections;
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

	//! Thread-local HT
	unique_ptr<JoinHashTable> hash_table;
};

unique_ptr<JoinHashTable> PhysicalHashJoin::InitializeHashTable(ClientContext &context) const {
	auto result =
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
			auto &info = result->correlated_mark_join_info;

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
	return result;
}

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<HashJoinGlobalSinkState>();
	state->hash_table = InitializeHashTable(context);

	// for perfect hash join
	state->perfect_join_executor =
	    make_unique<PerfectHashJoinExecutor>(*this, *state->hash_table, perfect_join_statistics);
	// for external hash join
	state->external = !recursive_cte && ClientConfig::GetConfig(context).force_external;
	// memory usage per thread scales with max mem / num threads
	double max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	double num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	// HT may not exceed 60% of memory
	state->max_ht_size = max_memory * 0.6;
	state->sink_memory_per_thread = state->max_ht_size / num_threads;
	// Set probe types
	auto &probe_types = state->probe_types;
	const auto &payload_types = children[0]->types;
	probe_types.insert(probe_types.end(), condition_types.begin(), condition_types.end());
	probe_types.insert(probe_types.end(), payload_types.begin(), payload_types.end());
	probe_types.emplace_back(LogicalType::HASH);

	return move(state);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto state = make_unique<HashJoinLocalSinkState>(allocator, *this);
	state->hash_table = InitializeHashTable(context.client);
	return move(state);
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                      DataChunk &input) const {
	auto &gstate = (HashJoinGlobalSinkState &)gstate_p;
	auto &lstate = (HashJoinLocalSinkState &)lstate_p;

	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.build_executor.Execute(input, lstate.join_keys);
	// TODO: add statement to check for possible per
	// build the HT
	auto &ht = *lstate.hash_table;
	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
		ht.Build(lstate.join_keys, lstate.build_chunk);
	} else if (!build_types.empty()) {
		// there is not a projected map: place the entire right chunk in the HT
		ht.Build(lstate.join_keys, input);
	} else {
		// there are only keys: place an empty chunk in the payload
		lstate.build_chunk.SetCardinality(input.size());
		ht.Build(lstate.join_keys, lstate.build_chunk);
	}

	// swizzle if we reach memory limit
	if (!recursive_cte &&
	    ht.SizeInBytes() + ht.PointerTableCapacity(ht.Count()) * sizeof(data_ptr_t) >= gstate.sink_memory_per_thread) {
		lstate.hash_table->SwizzleBlocks();
		gstate.external = true;
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (HashJoinGlobalSinkState &)gstate_p;
	auto &lstate = (HashJoinLocalSinkState &)lstate_p;
	if (lstate.hash_table) {
		lock_guard<mutex> local_ht_lock(gstate.lock);
		gstate.local_hash_tables.push_back(move(lstate.hash_table));
	}
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &lstate.build_executor, "build_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class HashJoinFinalizeTask : public ExecutorTask {
public:
	HashJoinFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, HashJoinGlobalSinkState &sink,
	                     idx_t block_idx_start, idx_t block_idx_end, bool parallel)
	    : ExecutorTask(context), event(move(event_p)), sink(sink), block_idx_start(block_idx_start),
	      block_idx_end(block_idx_end), parallel(parallel) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		sink.hash_table->Finalize(block_idx_start, block_idx_end, parallel);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	HashJoinGlobalSinkState &sink;
	idx_t block_idx_start;
	idx_t block_idx_end;
	bool parallel;
};

class HashJoinFinalizeEvent : public Event {
public:
	HashJoinFinalizeEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : Event(pipeline_p.executor), pipeline(pipeline_p), sink(sink) {
	}

	Pipeline &pipeline;
	HashJoinGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();
		auto parallel_construct_count =
		    context.config.verify_parallelism ? STANDARD_VECTOR_SIZE : PARALLEL_CONSTRUCT_COUNT;

		vector<unique_ptr<Task>> finalize_tasks;
		auto &ht = *sink.hash_table;
		const auto &block_collection = ht.GetBlockCollection();
		const auto &blocks = block_collection.blocks;
		const auto num_blocks = blocks.size();
		if (block_collection.count < parallel_construct_count) {
			// Single-threaded finalize
			finalize_tasks.push_back(
			    make_unique<HashJoinFinalizeTask>(shared_from_this(), context, sink, 0, num_blocks, false));
		} else {
			// Parallel finalize
			idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
			auto blocks_per_thread = MaxValue<idx_t>((num_blocks + num_threads - 1) / num_threads, 1);

			idx_t block_idx = 0;
			for (idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
				auto block_idx_start = block_idx;
				auto block_idx_end = MinValue<idx_t>(block_idx_start + blocks_per_thread, num_blocks);
				finalize_tasks.push_back(make_unique<HashJoinFinalizeTask>(shared_from_this(), context, sink,
				                                                           block_idx_start, block_idx_end, true));
				block_idx = block_idx_end;
				if (block_idx == num_blocks) {
					break;
				}
			}
		}
		SetTasks(move(finalize_tasks));
	}

	void FinishEvent() override {
		sink.hash_table->finalized = true;
	}

	// 1 << 18 TODO: tweak experimentally
	static constexpr const idx_t PARALLEL_CONSTRUCT_COUNT = 262144;
};

void ScheduleFinalize(Pipeline &pipeline, Event &event, HashJoinGlobalSinkState &sink) {
	sink.hash_table->InitializePointerTable();
	auto new_event = make_shared<HashJoinFinalizeEvent>(pipeline, sink);
	event.InsertEvent(move(new_event));
}

class HashJoinPartitionTask : public ExecutorTask {
public:
	HashJoinPartitionTask(shared_ptr<Event> event_p, ClientContext &context, JoinHashTable &global_ht,
	                      JoinHashTable &local_ht)
	    : ExecutorTask(context), event(move(event_p)), global_ht(global_ht), local_ht(local_ht) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		local_ht.Partition(global_ht);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;

	JoinHashTable &global_ht;
	JoinHashTable &local_ht;
};

class HashJoinPartitionEvent : public Event {
public:
	HashJoinPartitionEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink,
	                       vector<unique_ptr<JoinHashTable>> &local_hts)
	    : Event(pipeline_p.executor), pipeline(pipeline_p), sink(sink), local_hts(local_hts) {
	}

	Pipeline &pipeline;
	HashJoinGlobalSinkState &sink;
	vector<unique_ptr<JoinHashTable>> &local_hts;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();
		vector<unique_ptr<Task>> partition_tasks;
		partition_tasks.reserve(local_hts.size());
		for (auto &local_ht : local_hts) {
			partition_tasks.push_back(
			    make_unique<HashJoinPartitionTask>(shared_from_this(), context, *sink.hash_table, *local_ht));
		}
		SetTasks(move(partition_tasks));
	}

	void FinishEvent() override {
		local_hts.clear();
		sink.hash_table->PrepareExternalFinalize();
		ScheduleFinalize(pipeline, *this, sink);
	}
};

SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	auto &sink = (HashJoinGlobalSinkState &)gstate;

	if (sink.external) {
		D_ASSERT(!recursive_cte);
		// External join - partition HT
		sink.perfect_join_executor.reset();
		sink.hash_table->ComputePartitionSizes(pipeline, event, sink.local_hash_tables, sink.max_ht_size);
		auto new_event = make_shared<HashJoinPartitionEvent>(pipeline, sink, sink.local_hash_tables);
		event.InsertEvent(move(new_event));
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
		ScheduleFinalize(pipeline, event, sink);
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
	explicit PhysicalHashJoinState(Allocator &allocator) : probe_executor(allocator), spill_collection(nullptr) {
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

	//! Collection and chunk to sink data into for external join
	ColumnDataCollection *spill_collection;
	ColumnDataAppendState spill_append_state;
	DataChunk spill_chunk;

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
		state->spill_chunk.Initialize(allocator, sink.probe_types);
		lock_guard<mutex> local_ht_lock(sink.lock);
		sink.spill_collections.push_back(
		    make_unique<ColumnDataCollection>(BufferManager::GetBufferManager(context.client), sink.probe_types));
		state->spill_collection = sink.spill_collections.back().get();
		state->spill_collection->InitializeAppend(state->spill_append_state);
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
		state.scan_structure =
		    sink.hash_table->ProbeAndBuild(state.join_keys, input, *state.spill_collection, state.spill_chunk);
	} else {
		state.scan_structure = sink.hash_table->Probe(state.join_keys);
	}
	state.scan_structure->Next(state.join_keys, input, chunk);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashJoinLocalSourceState;

class HashJoinGlobalSourceState : public GlobalSourceState {
public:
	HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context)
	    : op(op), initialized(false), parallel_scan_chunk_count(context.config.verify_parallelism ? 1 : 120),
	      chunk_idx(0), chunk_count(0), chunk_done(0), probe_count(op.children[0]->estimated_cardinality) {
	}

	const PhysicalHashJoin &op;

	mutex source_lock;
	atomic<bool> initialized;

	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState full_outer_scan_state;

	//! Only used for external join: Materialized probe-side data and scan structure
	unique_ptr<ColumnDataCollection> probe_collection;
	idx_t parallel_scan_chunk_count;
	idx_t chunk_idx;
	atomic<idx_t> chunk_count;
	atomic<idx_t> chunk_done;

	void Initialize(HashJoinGlobalSinkState &sink) {
		if (initialized) {
			return;
		}
		lock_guard<mutex> lock(sink.lock);
		if (initialized) {
			// Have to check if anything changed
			return;
		}
		full_outer_scan_state.total = sink.hash_table->Count();
		probe_collection = make_unique<ColumnDataCollection>(sink.hash_table->buffer_manager, sink.probe_types);
		initialized = true;
	}

	void AssignProbeChunks(HashJoinLocalSourceState &lstate);

	idx_t probe_count;
	idx_t MaxThreads() override {
		return probe_count / ((idx_t)STANDARD_VECTOR_SIZE * parallel_scan_chunk_count);
	}
};

//! Only used for external hash join
class HashJoinLocalSourceState : public LocalSourceState {
public:
	HashJoinLocalSourceState(const PhysicalHashJoin &op, Allocator &allocator)
	    : addresses(LogicalType::POINTER), chunk_idx(0), chunk_idx_end(0), full_outer_in_progress(0) {
		auto &sink = (HashJoinGlobalSinkState &)*op.sink_state;
		probe_chunk.Initialize(allocator, sink.probe_types);
		join_keys.Initialize(allocator, op.condition_types);
		payload.Initialize(allocator, op.children[0]->types);
	}

public:
	//! Stores chunks from the probe collection
	DataChunk probe_chunk;
	//! Stores the probe keys
	DataChunk join_keys;
	//! Stores the other probe columns
	DataChunk payload;
	//! Scan structure for the external probe
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;

	//! Vector with pointers here so we don't have to re-initialize
	Vector addresses;

public:
	idx_t chunk_idx;
	idx_t chunk_idx_end;

	//! Current number of tuples from a full/outer scan that are 'in-flight'
	idx_t full_outer_in_progress;
};

void HashJoinGlobalSourceState::AssignProbeChunks(HashJoinLocalSourceState &lstate) {
	lock_guard<mutex> lock(source_lock);
	if (chunk_idx == chunk_count) {
		return;
	}

	auto chunk_idx_end = MinValue<idx_t>(chunk_idx + parallel_scan_chunk_count, chunk_count);

	lstate.chunk_idx = chunk_idx;
	lstate.chunk_idx_end = chunk_idx_end;

	chunk_idx = chunk_idx_end;
}

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<HashJoinGlobalSourceState>(*this, context);
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_unique<HashJoinLocalSourceState>(*this, Allocator::Get(context.client));
}

void PartitionProbeSide(HashJoinGlobalSourceState &gstate, HashJoinGlobalSinkState &sink) {
	lock_guard<mutex> lock(gstate.source_lock);
	if (sink.spill_collections.empty()) {
		return;
	}
	// For now we actually don't partition the probe side TODO
	for (auto &spill_collection : sink.spill_collections) {
		gstate.probe_collection->Combine(*spill_collection);
	}
	gstate.chunk_count = gstate.probe_collection->ChunkCount();

	// Because we don't actually partition, we have to mark the current probe phase as done TODO
	D_ASSERT(gstate.chunk_idx == 0 && gstate.chunk_done == 0);
	gstate.chunk_idx += gstate.chunk_count;
	gstate.chunk_done += gstate.chunk_count;

	sink.spill_collections.clear();
}

bool PhysicalHashJoin::PreparePartitionedRound(HashJoinGlobalSourceState &gstate) const {
	// Prepare the build side
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	auto &ht = *sink.hash_table;
	if (!ht.PrepareExternalFinalize()) {
		// Done
		return false;
	}
	ht.InitializePointerTable();
	// TODO: parallelize the finalize!
	ht.Finalize(0, ht.GetBlockCollection().blocks.size(), false);
	ht.finalized = true;

	// Prepare the probe side TODO
	gstate.chunk_idx = 0;
	gstate.chunk_done = 0;

	// Reset full outer scan state (if necessary)
	if (IsRightOuterJoin(join_type)) {
		gstate.full_outer_scan_state.Reset();
		gstate.full_outer_scan_state.total = ht.Count();
	}

	return true;
}

//! Probe the build side using the materialized and partitioned probe side
void ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, HashJoinLocalSourceState &lstate,
                   DataChunk &chunk) {
	D_ASSERT(sink.hash_table->finalized);

	// Construct input Chunk for next probe
	lstate.probe_chunk.Reset();
	gstate.probe_collection->FetchChunk(lstate.chunk_idx++, lstate.probe_chunk);
	// Get keys
	const auto key_count = lstate.join_keys.ColumnCount();
	lstate.join_keys.Reset();
	for (idx_t col_idx = 0; col_idx < key_count; col_idx++) {
		lstate.join_keys.data[col_idx].Reference(lstate.probe_chunk.data[col_idx]);
	}
	lstate.join_keys.SetCardinality(lstate.probe_chunk.size());
	// Get payload
	lstate.payload.Reset();
	for (idx_t col_idx = 0; col_idx < lstate.payload.ColumnCount(); col_idx++) {
		lstate.payload.data[col_idx].Reference(lstate.probe_chunk.data[key_count + col_idx]);
	}
	lstate.payload.SetCardinality(lstate.probe_chunk.size());

	// Perform the probe (we already have the hashes)
	lstate.scan_structure = sink.hash_table->Probe(lstate.join_keys, &lstate.probe_chunk.data.back());
	lstate.scan_structure->Next(lstate.join_keys, lstate.payload, chunk);
}

void PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                               LocalSourceState &lstate_p) const {
	D_ASSERT(!recursive_cte);
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	auto &gstate = (HashJoinGlobalSourceState &)gstate_p;
	auto &lstate = (HashJoinLocalSourceState &)lstate_p;

	auto &fo_ss = gstate.full_outer_scan_state;
	if (!sink.external) {
		if (IsRightOuterJoin(join_type)) {
			// Check if we need to scan any unmatched tuples from the RHS for the full/right outer join
			idx_t found_entries;
			{
				lock_guard<mutex> lock(gstate.source_lock);
				found_entries = sink.hash_table->ScanFullOuter(fo_ss, lstate.addresses);
			}
			sink.hash_table->GatherFullOuter(chunk, lstate.addresses, found_entries);
		}
		return;
	}

	// Initialize global source state (if not yet done)
	gstate.Initialize(sink);

	// Partition probe side (if not yet done)
	PartitionProbeSide(gstate, sink);

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	while (chunk.size() == 0) {
		if (lstate.scan_structure) {
			// Still have elements remaining from the previous probe (i.e. we got >1024 elements in the previous probe)
			lstate.scan_structure->Next(lstate.join_keys, lstate.payload, chunk);
			if (chunk.size() != 0) {
				return;
			}
			// This scan structure is done, reset it and increment scan progress
			lstate.scan_structure = nullptr;
			gstate.chunk_done++;
		}

		if (lstate.chunk_idx != lstate.chunk_idx_end) {
			// Probe using the current blocks assigned to this thread
			ExternalProbe(sink, gstate, lstate, chunk);
			continue;
		}

		// This thread completed its blocks, try to assign new ones
		if (lstate.chunk_idx == lstate.chunk_idx_end) {
			gstate.AssignProbeChunks(lstate);
			if (lstate.chunk_idx != lstate.chunk_idx_end) {
				// We assigned new blocks to this thread
				continue;
			}
		}

		// If we arrived here, we've run out of blocks to assign to threads
		if (gstate.chunk_done != gstate.chunk_count) {
			// At least one thread is still probing, try again next iteration
			continue;
		}

		if (lstate.full_outer_in_progress != 0) {
			// This thread did a full/outer scan, increment scan progress
			D_ASSERT(IsRightOuterJoin(join_type));
			fo_ss.scanned += lstate.full_outer_in_progress;
			lstate.full_outer_in_progress = 0;
		}

		// Now we either scan full/outer, or prepare the next probe round
		idx_t found_entries;
		do { // 'do' just so we can 'break' to release the lock
			lock_guard<mutex> lock(gstate.source_lock);
			if (gstate.chunk_done != gstate.chunk_count) {
				// Another thread prepared the next probe round while we waited for the lock
				break;
			}

			if (IsRightOuterJoin(join_type) && fo_ss.scanned != fo_ss.total) {
				// We need to scan the HT for full/outer
				idx_t scan_index_before = fo_ss.scan_index;
				found_entries = sink.hash_table->ScanFullOuter(fo_ss, lstate.addresses);
				idx_t scanned = fo_ss.scan_index - scan_index_before;

				// Mark the assigned tuples as in-progress
				D_ASSERT(lstate.full_outer_in_progress == 0);
				lstate.full_outer_in_progress = scanned;
				break;
			}

			// Probe round is done, and full/outer scan is done too, try to prepare the next probe round
			if (PreparePartitionedRound(gstate)) {
				break;
			}

			// We could not prepare the next partitioned probe round, all partitions must be done
			return;
		} while (true);

		if (lstate.full_outer_in_progress != 0) {
			// We assigned this thread to scan the HT
			D_ASSERT(IsRightOuterJoin(join_type));
			sink.hash_table->GatherFullOuter(chunk, lstate.addresses, found_entries);
		}
	}
}

} // namespace duckdb
