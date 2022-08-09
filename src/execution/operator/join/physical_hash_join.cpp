#include "duckdb/execution/operator/join/physical_hash_join.hpp"

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
	idx_t execute_memory_per_thread;

	//! Hash tables built by each thread
	mutex lock;
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
	// during the Sink phase we spill at 60%
	state->max_ht_size = max_memory * 0.6;
	state->sink_memory_per_thread = state->max_ht_size / num_threads;
	// during the Execute phase we are probing and sinking at the same time
	// at this point we're already doing an external join, and we need to spill the new sink data
	// we can spill at a much lower percentage, 10%
	state->execute_memory_per_thread = max_memory / num_threads * 0.1;
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
	if (!recursive_cte && ht.SizeInBytes() + ht.PointerTableSize(ht.Count()) >= gstate.sink_memory_per_thread) {
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
void ScheduleFinalize(Pipeline &pipeline, Event &event, HashJoinGlobalSinkState &sink);

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
	HashJoinPartitionEvent(Pipeline &pipeline_p, Event &event_p, HashJoinGlobalSinkState &sink,
	                       vector<unique_ptr<JoinHashTable>> &local_hts)
	    : Event(pipeline_p.executor), pipeline(pipeline_p), event(event_p), sink(sink), local_hts(local_hts) {
	}

	Pipeline &pipeline;
	Event &event;
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
		ScheduleFinalize(pipeline, event, sink);
	}
};

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
	HashJoinFinalizeEvent(Pipeline &pipeline_p, Event &event_p, HashJoinGlobalSinkState &sink)
	    : Event(pipeline_p.executor), pipeline(pipeline_p), event(event_p), sink(sink) {
	}

	Pipeline &pipeline;
	Event &event;
	HashJoinGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();
		auto hashes_per_thread = context.config.verify_parallelism ? STANDARD_VECTOR_SIZE : PARALLEL_HASHES_PER_THREAD;

		vector<unique_ptr<Task>> finalize_tasks;
		auto &ht = *sink.hash_table;
		const auto &block_collection = ht.GetBlockCollection();
		const auto &blocks = block_collection.blocks;
		const auto num_blocks = blocks.size();
		if (block_collection.count < hashes_per_thread) {
			// No need to parallelize
			finalize_tasks.push_back(
			    make_unique<HashJoinFinalizeTask>(shared_from_this(), context, sink, 0, num_blocks, false));
		} else {
			// Fixed amount of work per task
			idx_t block_idx;
			for (block_idx = 0; block_idx < num_blocks;) {
				auto block_idx_start = block_idx;
				idx_t count = 0;
				for (; block_idx < num_blocks; block_idx++) {
					count += blocks[block_idx]->count;
					if (count >= hashes_per_thread) {
						block_idx++;
						break;
					}
				}
				auto block_idx_end = block_idx;
				finalize_tasks.push_back(make_unique<HashJoinFinalizeTask>(shared_from_this(), context, sink,
				                                                           block_idx_start, block_idx_end, true));
			}
		}
		SetTasks(move(finalize_tasks));
	}

	void FinishEvent() override {
		sink.hash_table->finalized = true;
	}

	// 1 << 16
	static constexpr const idx_t PARALLEL_HASHES_PER_THREAD = 262144;
};

void ScheduleFinalize(Pipeline &pipeline, Event &event, HashJoinGlobalSinkState &sink) {
	sink.hash_table->InitializePointerTable();
	auto new_event = make_shared<HashJoinFinalizeEvent>(pipeline, event, sink);
	event.InsertEvent(move(new_event));
}

SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	auto &sink = (HashJoinGlobalSinkState &)gstate;

	if (sink.external) {
		D_ASSERT(!recursive_cte);
		// External join - partition HT
		sink.perfect_join_executor.reset();
		sink.hash_table->ComputePartitionSizes(pipeline, event, sink.local_hash_tables, sink.max_ht_size);
		auto new_event = make_shared<HashJoinPartitionEvent>(pipeline, event, sink, sink.local_hash_tables);
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

		lock_guard<mutex> local_ht_lock(sink.lock);
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
class HashJoinLocalSourceState;

class HashJoinGlobalSourceState : public GlobalSourceState {
public:
	HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context)
	    : op(op), probe_ht(make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), op.conditions,
	                                                  op.children[0]->types, op.join_type)),
	      probe_count(op.children[0]->estimated_cardinality),
	      parallel_scan_vector_count(context.config.verify_parallelism ? 1 : 120), initialized(false),
	      local_hts_done(0) {
	}

	const PhysicalHashJoin &op;

	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState full_outer_scan_state;

	//! Only used for external join: Materialized probe-side data and scan structure
	unique_ptr<JoinHashTable> probe_ht;
	idx_t probe_count;
	idx_t parallel_scan_vector_count;
	JoinHTScanState probe_ss;

	mutex source_lock;
	atomic<bool> initialized;
	idx_t local_ht_count;
	idx_t local_hts_done;

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
		probe_ht->radix_bits = sink.hash_table->radix_bits;
		local_ht_count = sink.local_hash_tables.size();
		initialized = true;
	}

	void AssignProbeBlocks(HashJoinLocalSourceState &lstate);

	idx_t MaxThreads() override {
		return probe_count / ((idx_t)STANDARD_VECTOR_SIZE * parallel_scan_vector_count);
	}
};

//! Only used for external join
class HashJoinLocalSourceState : public LocalSourceState {
public:
	HashJoinLocalSourceState(const PhysicalHashJoin &op, Allocator &allocator)
	    : addresses(LogicalType::POINTER), block_idx(0), entry_idx(0), block_idx_deleted(0), block_idx_end(0),
	      full_outer_in_progress(0) {
		join_keys.Initialize(allocator, op.condition_types);
		payload.Initialize(allocator, op.children[0]->types);
	}

public:
	//! Same as the input chunk in PhysicalHashJoin::Execute
	DataChunk join_keys;
	DataChunk payload;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;

	//! Vector with pointers here so we don't have to re-initialize
	Vector addresses;

public:
	idx_t block_idx;
	idx_t entry_idx;
	idx_t block_idx_deleted;
	idx_t block_idx_end;

	//! Current number of tuples from a full/outer scan that are 'in-flight'
	idx_t full_outer_in_progress;
};

void HashJoinGlobalSourceState::AssignProbeBlocks(duckdb::HashJoinLocalSourceState &lstate) {
	lock_guard<mutex> lock(source_lock);
	const auto &block_collection = probe_ht->GetBlockCollection();

	lstate.block_idx_deleted = probe_ss.block_position;
	lstate.block_idx = probe_ss.block_position;
	idx_t total = 0;
	for (; probe_ss.block_position < block_collection.blocks.size(); probe_ss.block_position++) {
		auto &block = *block_collection.blocks[probe_ss.block_position];
		if (block.count == 0) {
			// Skip over empty blocks just in case
			continue;
		}
		total += block.count;
		if (total >= idx_t(parallel_scan_vector_count * STANDARD_VECTOR_SIZE)) {
			break;
		}
	}
	lstate.block_idx_end = probe_ss.block_position;
}

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<HashJoinGlobalSourceState>(*this, context);
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_unique<HashJoinLocalSourceState>(*this, Allocator::Get(context.client));
}

void PartitionProbeSide(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate) {
	{
		lock_guard<mutex> local_ht_lock(sink.lock);
		if (gstate.local_hts_done == gstate.local_ht_count) {
			return;
		}

		auto &build_ht = *sink.hash_table;
		if (build_ht.total_count - build_ht.Count() <= build_ht.tuples_per_round) {
			// If there will only be one more probe round, we don't need to partition the probe side
			for (auto &local_ht : sink.local_hash_tables) {
				gstate.probe_ht->Merge(*local_ht);
			}

			sink.local_hash_tables.clear();
			gstate.local_hts_done = gstate.local_ht_count;
			return;
		}
	}
	while (true) {
		unique_ptr<JoinHashTable> local_ht;
		{
			// Partition/spinlock until all partitioning is done
			lock_guard<mutex> local_ht_lock(sink.lock);
			if (gstate.local_hts_done == gstate.local_ht_count) {
				break;
			}
			if (sink.local_hash_tables.empty()) {
				continue;
			}
			local_ht = move(sink.local_hash_tables.back());
			sink.local_hash_tables.pop_back();
		}
		// Partition after releasing the lock, grab lock again to update state
		local_ht->Partition(*gstate.probe_ht);
		lock_guard<mutex> local_ht_lock(sink.lock);
		gstate.local_hts_done++;
	}
}

bool PhysicalHashJoin::PreparePartitionedRound(HashJoinGlobalSourceState &gstate) const {
	auto &probe_ss = gstate.probe_ss;
	D_ASSERT(probe_ss.scanned == probe_ss.total);

	// Prepare the build side
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	sink.hash_table->PrepareExternalFinalize();
	if (!sink.hash_table->finalized) {
		// Done
		return false;
	}

	// Prepare the probe side
	gstate.probe_ht->PreparePartitionedProbe(*sink.hash_table, probe_ss);
	// Reset full outer scan state (if necessary)
	if (IsRightOuterJoin(join_type)) {
		gstate.full_outer_scan_state.Reset();
		gstate.full_outer_scan_state.total = sink.hash_table->Count();
	}

	return true;
}

//! Probe the build side using the materialized and partitioned probe side
void ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, HashJoinLocalSourceState &lstate,
                   DataChunk &chunk) {
	D_ASSERT(sink.hash_table->finalized);

	// Construct input Chunk for next probe
	gstate.probe_ht->GatherProbeTuples(lstate.join_keys, lstate.payload, lstate.addresses, lstate.block_idx,
	                                   lstate.entry_idx, lstate.block_idx_deleted, lstate.block_idx_end);

	// Perform the probe
	// TODO optimization: we already have the hashes
	// TODO optimization: collect hashes first, match them
	//  then, we collect the key columns only of rows that had a matching hash, and try to match them
	//  finally, we collect only the payload columns of the rows that had matching key columns
	//  this saves a ton of memcpy's, especially if we have many payload columns
	lstate.scan_structure = sink.hash_table->Probe(lstate.join_keys);
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

	// This is an external hash join
	D_ASSERT(gstate.probe_ht);

	// Initialize global source state (if not yet done)
	gstate.Initialize(sink);

	// Partition probe-side data (if not yet done)
	PartitionProbeSide(sink, gstate);

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	auto &probe_ss = gstate.probe_ss;
	while (chunk.size() == 0) {
		if (lstate.scan_structure) {
			// Still have elements remaining from the previous probe (i.e. we got >1024 elements in the previous probe)
			lstate.scan_structure->Next(lstate.join_keys, lstate.payload, chunk);
			if (chunk.size() != 0) {
				return;
			}
			// This scan structure is done, reset it and increment scan progress
			lstate.scan_structure = nullptr;
			probe_ss.scanned += lstate.join_keys.size();
		}

		if (lstate.block_idx != lstate.block_idx_end) {
			// Probe using the current blocks assigned to this thread
			ExternalProbe(sink, gstate, lstate, chunk);
			continue;
		}

		// This thread completed its blocks, try to assign new ones
		if (lstate.block_idx == lstate.block_idx_end) {
			gstate.AssignProbeBlocks(lstate);
			if (lstate.block_idx != lstate.block_idx_end) {
				// We assigned new blocks to this thread
				continue;
			}
		}

		// If we arrived here, we've run out of blocks to assign to threads
		if (probe_ss.scanned != probe_ss.total) {
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
			if (probe_ss.scanned != probe_ss.total) {
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
