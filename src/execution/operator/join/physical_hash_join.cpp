#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   const vector<idx_t> &left_projection_map, const vector<idx_t> &right_projection_map,
                                   vector<LogicalType> delim_types, idx_t estimated_cardinality,
                                   PerfectHashJoinStats perfect_join_stats,
                                   unique_ptr<JoinFilterPushdownInfo> pushdown_info_p)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, std::move(cond), join_type, estimated_cardinality),
      delim_types(std::move(delim_types)), perfect_join_statistics(std::move(perfect_join_stats)) {
	D_ASSERT(left_projection_map.empty());

	filter_pushdown = std::move(pushdown_info_p);

	children.push_back(std::move(left));
	children.push_back(std::move(right));

	// Collect condition types, and which conditions are just references (so we won't duplicate them in the payload)
	unordered_map<idx_t, idx_t> build_columns_in_conditions;
	for (idx_t cond_idx = 0; cond_idx < conditions.size(); cond_idx++) {
		auto &condition = conditions[cond_idx];
		condition_types.push_back(condition.left->return_type);
		if (condition.right->GetExpressionClass() == ExpressionClass::BOUND_REF) {
			build_columns_in_conditions.emplace(condition.right->Cast<BoundReferenceExpression>().index, cond_idx);
		}
	}

	// For ANTI, SEMI and MARK join, we only need to store the keys, so for these the payload/RHS types are empty
	if (join_type == JoinType::ANTI || join_type == JoinType::SEMI || join_type == JoinType::MARK) {
		return;
	}

	auto &rhs_input_types = children[1]->GetTypes();

	// Create a projection map for the RHS (if it was empty), for convenience
	auto right_projection_map_copy = right_projection_map;
	if (right_projection_map_copy.empty()) {
		right_projection_map_copy.reserve(rhs_input_types.size());
		for (idx_t i = 0; i < rhs_input_types.size(); i++) {
			right_projection_map_copy.emplace_back(i);
		}
	}

	// Now fill payload expressions/types and RHS columns/types
	for (auto &rhs_col : right_projection_map_copy) {
		auto &rhs_col_type = rhs_input_types[rhs_col];

		auto it = build_columns_in_conditions.find(rhs_col);
		if (it == build_columns_in_conditions.end()) {
			// This rhs column is not a join key
			payload_column_idxs.push_back(rhs_col);
			payload_types.push_back(rhs_col_type);
			rhs_output_columns.push_back(condition_types.size() + payload_types.size() - 1);
		} else {
			// This rhs column is a join key
			rhs_output_columns.push_back(it->second);
		}
		rhs_output_types.push_back(rhs_col_type);
	}
}

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_state)
    : PhysicalHashJoin(op, std::move(left), std::move(right), std::move(cond), join_type, {}, {}, {},
                       estimated_cardinality, std::move(perfect_join_state), nullptr) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
JoinFilterGlobalState::~JoinFilterGlobalState() {
}

JoinFilterLocalState::~JoinFilterLocalState() {
}

unique_ptr<JoinFilterGlobalState> JoinFilterPushdownInfo::GetGlobalState(ClientContext &context,
                                                                         const PhysicalOperator &op) const {
	// clear any previously set filters
	// we can have previous filters for this operator in case of e.g. recursive CTEs
	dynamic_filters->ClearFilters(op);
	auto result = make_uniq<JoinFilterGlobalState>();
	result->global_aggregate_state =
	    make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context), min_max_aggregates);
	return result;
}

class HashJoinGlobalSinkState : public GlobalSinkState {
public:
	HashJoinGlobalSinkState(const PhysicalHashJoin &op_p, ClientContext &context_p)
	    : context(context_p), op(op_p),
	      num_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
	      temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)), finalized(false),
	      active_local_states(0), total_size(0), max_partition_size(0), max_partition_count(0), scanned_data(false) {
		hash_table = op.InitializeHashTable(context);

		// For perfect hash join
		perfect_join_executor = make_uniq<PerfectHashJoinExecutor>(op, *hash_table, op.perfect_join_statistics);
		// For external hash join
		external = ClientConfig::GetConfig(context).force_external;
		// Set probe types
		const auto &payload_types = op.children[0]->types;
		probe_types.insert(probe_types.end(), op.condition_types.begin(), op.condition_types.end());
		probe_types.insert(probe_types.end(), payload_types.begin(), payload_types.end());
		probe_types.emplace_back(LogicalType::HASH);

		if (op.filter_pushdown) {
			global_filter_state = op.filter_pushdown->GetGlobalState(context, op);
		}
	}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);
	void InitializeProbeSpill();

public:
	ClientContext &context;
	const PhysicalHashJoin &op;

	const idx_t num_threads;
	//! Temporary memory state for managing this operator's memory usage
	unique_ptr<TemporaryMemoryState> temporary_memory_state;

	//! Global HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized;
	//! The number of active local states
	atomic<idx_t> active_local_states;

	//! Whether we are doing an external + some sizes
	bool external;
	idx_t total_size;
	idx_t max_partition_size;
	idx_t max_partition_count;

	//! Hash tables built by each thread
	vector<unique_ptr<JoinHashTable>> local_hash_tables;

	//! Excess probe data gathered during Sink
	vector<LogicalType> probe_types;
	unique_ptr<JoinHashTable::ProbeSpill> probe_spill;

	//! Whether or not we have started scanning data using GetData
	atomic<bool> scanned_data;

	unique_ptr<JoinFilterGlobalState> global_filter_state;
};

unique_ptr<JoinFilterLocalState> JoinFilterPushdownInfo::GetLocalState(JoinFilterGlobalState &gstate) const {
	auto result = make_uniq<JoinFilterLocalState>();
	result->local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*gstate.global_aggregate_state);
	return result;
}

class HashJoinLocalSinkState : public LocalSinkState {
public:
	HashJoinLocalSinkState(const PhysicalHashJoin &op, ClientContext &context, HashJoinGlobalSinkState &gstate)
	    : join_key_executor(context) {
		auto &allocator = BufferAllocator::Get(context);

		for (auto &cond : op.conditions) {
			join_key_executor.AddExpression(*cond.right);
		}
		join_keys.Initialize(allocator, op.condition_types);

		if (!op.payload_types.empty()) {
			payload_chunk.Initialize(allocator, op.payload_types);
		}

		hash_table = op.InitializeHashTable(context);
		hash_table->GetSinkCollection().InitializeAppendState(append_state);

		gstate.active_local_states++;

		if (op.filter_pushdown) {
			local_filter_state = op.filter_pushdown->GetLocalState(*gstate.global_filter_state);
		}
	}

public:
	PartitionedTupleDataAppendState append_state;

	ExpressionExecutor join_key_executor;
	DataChunk join_keys;

	DataChunk payload_chunk;

	//! Thread-local HT
	unique_ptr<JoinHashTable> hash_table;

	unique_ptr<JoinFilterLocalState> local_filter_state;
};

unique_ptr<JoinHashTable> PhysicalHashJoin::InitializeHashTable(ClientContext &context) const {
	auto result = make_uniq<JoinHashTable>(context, conditions, payload_types, join_type, rhs_output_columns);
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

			vector<LogicalType> delim_payload_types;
			vector<BoundAggregateExpression *> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs

			FunctionBinder function_binder(context);
			aggr = function_binder.BindAggregateFunction(CountStarFun::GetFunction(), {}, nullptr,
			                                             AggregateType::NON_DISTINCT);
			correlated_aggregates.push_back(&*aggr);
			delim_payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(std::move(aggr));

			auto count_fun = CountFun::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_uniq_base<Expression, BoundReferenceExpression>(count_fun.return_type, 0U));
			aggr = function_binder.BindAggregateFunction(count_fun, std::move(children), nullptr,
			                                             AggregateType::NON_DISTINCT);
			correlated_aggregates.push_back(&*aggr);
			delim_payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(std::move(aggr));

			auto &allocator = BufferAllocator::Get(context);
			info.correlated_counts = make_uniq<GroupedAggregateHashTable>(context, allocator, delim_types,
			                                                              delim_payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(allocator, delim_types);
			info.result_chunk.Initialize(allocator, delim_payload_types);
		}
	}
	return result;
}

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HashJoinGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<HashJoinGlobalSinkState>();
	return make_uniq<HashJoinLocalSinkState>(*this, context.client, gstate);
}

void JoinFilterPushdownInfo::Sink(DataChunk &chunk, JoinFilterLocalState &lstate) const {
	// if we are pushing any filters into a probe-side, compute the min/max over the columns that we are pushing
	for (idx_t pushdown_idx = 0; pushdown_idx < filters.size(); pushdown_idx++) {
		auto &pushdown = filters[pushdown_idx];
		for (idx_t i = 0; i < 2; i++) {
			idx_t aggr_idx = pushdown_idx * 2 + i;
			lstate.local_aggregate_state->Sink(chunk, pushdown.join_condition, aggr_idx);
		}
	}
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<HashJoinLocalSinkState>();

	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.join_key_executor.Execute(chunk, lstate.join_keys);

	if (filter_pushdown) {
		filter_pushdown->Sink(lstate.join_keys, *lstate.local_filter_state);
	}

	// build the HT
	auto &ht = *lstate.hash_table;
	if (payload_types.empty()) {
		// there are only keys: place an empty chunk in the payload
		lstate.payload_chunk.SetCardinality(chunk.size());
		ht.Build(lstate.append_state, lstate.join_keys, lstate.payload_chunk);
	} else {
		// there are payload columns
		lstate.payload_chunk.Reset();
		lstate.payload_chunk.SetCardinality(chunk);
		for (idx_t i = 0; i < payload_column_idxs.size(); i++) {
			lstate.payload_chunk.data[i].Reference(chunk.data[payload_column_idxs[i]]);
		}
		ht.Build(lstate.append_state, lstate.join_keys, lstate.payload_chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void JoinFilterPushdownInfo::Combine(JoinFilterGlobalState &gstate, JoinFilterLocalState &lstate) const {
	gstate.global_aggregate_state->Combine(*lstate.local_aggregate_state);
}

SinkCombineResultType PhysicalHashJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSinkState>();

	lstate.hash_table->GetSinkCollection().FlushAppendState(lstate.append_state);
	auto guard = gstate.Lock();
	gstate.local_hash_tables.push_back(std::move(lstate.hash_table));
	if (gstate.local_hash_tables.size() == gstate.active_local_states) {
		// Set to 0 until PrepareFinalize
		gstate.temporary_memory_state->SetZero();
	}

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);
	if (filter_pushdown) {
		filter_pushdown->Combine(*gstate.global_filter_state, *lstate.local_filter_state);
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
static idx_t GetTupleWidth(const vector<LogicalType> &types, bool &all_constant) {
	idx_t tuple_width = 0;
	all_constant = true;
	for (auto &type : types) {
		tuple_width += GetTypeIdSize(type.InternalType());
		all_constant &= TypeIsConstantSize(type.InternalType());
	}
	return tuple_width + AlignValue(types.size()) / 8 + GetTypeIdSize(PhysicalType::UINT64);
}

static idx_t GetPartitioningSpaceRequirement(ClientContext &context, const vector<LogicalType> &types,
                                             const idx_t radix_bits, const idx_t num_threads) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	bool all_constant;
	idx_t tuple_width = GetTupleWidth(types, all_constant);

	auto tuples_per_block = buffer_manager.GetBlockSize() / tuple_width;
	auto blocks_per_chunk = (STANDARD_VECTOR_SIZE + tuples_per_block) / tuples_per_block + 1;
	if (!all_constant) {
		blocks_per_chunk += 2;
	}
	auto size_per_partition = blocks_per_chunk * buffer_manager.GetBlockAllocSize();
	auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);

	return num_threads * num_partitions * size_per_partition;
}

void PhysicalHashJoin::PrepareFinalize(ClientContext &context, GlobalSinkState &global_state) const {
	auto &gstate = global_state.Cast<HashJoinGlobalSinkState>();
	auto &ht = *gstate.hash_table;
	gstate.total_size =
	    ht.GetTotalSize(gstate.local_hash_tables, gstate.max_partition_size, gstate.max_partition_count);
	bool all_constant;
	gstate.temporary_memory_state->SetMaterializationPenalty(GetTupleWidth(children[0]->types, all_constant));
	gstate.temporary_memory_state->SetRemainingSize(gstate.total_size);
}

class HashJoinFinalizeTask : public ExecutorTask {
public:
	HashJoinFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, HashJoinGlobalSinkState &sink_p,
	                     idx_t chunk_idx_from_p, idx_t chunk_idx_to_p, bool parallel_p, const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), sink(sink_p), chunk_idx_from(chunk_idx_from_p),
	      chunk_idx_to(chunk_idx_to_p), parallel(parallel_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		sink.hash_table->Finalize(chunk_idx_from, chunk_idx_to, parallel);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	HashJoinGlobalSinkState &sink;
	idx_t chunk_idx_from;
	idx_t chunk_idx_to;
	bool parallel;
};

class HashJoinFinalizeEvent : public BasePipelineEvent {
public:
	HashJoinFinalizeEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	HashJoinGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> finalize_tasks;
		auto &ht = *sink.hash_table;
		const auto chunk_count = ht.GetDataCollection().ChunkCount();
		const auto num_threads = NumericCast<idx_t>(sink.num_threads);
		if (num_threads == 1 || (ht.Count() < PARALLEL_CONSTRUCT_THRESHOLD && !context.config.verify_parallelism)) {
			// Single-threaded finalize
			finalize_tasks.push_back(
			    make_uniq<HashJoinFinalizeTask>(shared_from_this(), context, sink, 0U, chunk_count, false, sink.op));
		} else {
			// Parallel finalize
			auto chunks_per_thread = MaxValue<idx_t>((chunk_count + num_threads - 1) / num_threads, 1);

			idx_t chunk_idx = 0;
			for (idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
				auto chunk_idx_from = chunk_idx;
				auto chunk_idx_to = MinValue<idx_t>(chunk_idx_from + chunks_per_thread, chunk_count);
				finalize_tasks.push_back(make_uniq<HashJoinFinalizeTask>(shared_from_this(), context, sink,
				                                                         chunk_idx_from, chunk_idx_to, true, sink.op));
				chunk_idx = chunk_idx_to;
				if (chunk_idx == chunk_count) {
					break;
				}
			}
		}
		SetTasks(std::move(finalize_tasks));
	}

	void FinishEvent() override {
		sink.hash_table->GetDataCollection().VerifyEverythingPinned();
		sink.hash_table->finalized = true;
	}

	static constexpr const idx_t PARALLEL_CONSTRUCT_THRESHOLD = 1048576;
};

void HashJoinGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	if (hash_table->Count() == 0) {
		hash_table->finalized = true;
		return;
	}
	hash_table->InitializePointerTable();
	auto new_event = make_shared_ptr<HashJoinFinalizeEvent>(pipeline, *this);
	event.InsertEvent(std::move(new_event));
}

void HashJoinGlobalSinkState::InitializeProbeSpill() {
	auto guard = Lock();
	if (!probe_spill) {
		probe_spill = make_uniq<JoinHashTable::ProbeSpill>(*hash_table, context, probe_types);
	}
}

class HashJoinRepartitionTask : public ExecutorTask {
public:
	HashJoinRepartitionTask(shared_ptr<Event> event_p, ClientContext &context, JoinHashTable &global_ht,
	                        JoinHashTable &local_ht, const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), global_ht(global_ht), local_ht(local_ht) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		local_ht.Repartition(global_ht);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	JoinHashTable &global_ht;
	JoinHashTable &local_ht;
};

class HashJoinRepartitionEvent : public BasePipelineEvent {
public:
	HashJoinRepartitionEvent(Pipeline &pipeline_p, const PhysicalHashJoin &op_p, HashJoinGlobalSinkState &sink,
	                         vector<unique_ptr<JoinHashTable>> &local_hts)
	    : BasePipelineEvent(pipeline_p), op(op_p), sink(sink), local_hts(local_hts) {
	}

	const PhysicalHashJoin &op;
	HashJoinGlobalSinkState &sink;
	vector<unique_ptr<JoinHashTable>> &local_hts;

public:
	void Schedule() override {
		D_ASSERT(sink.hash_table->GetRadixBits() > JoinHashTable::INITIAL_RADIX_BITS);
		auto block_size = sink.hash_table->buffer_manager.GetBlockSize();

		idx_t total_size = 0;
		idx_t total_count = 0;
		for (auto &local_ht : local_hts) {
			auto &sink_collection = local_ht->GetSinkCollection();
			total_size += sink_collection.SizeInBytes();
			total_count += sink_collection.Count();
		}
		auto total_blocks = (total_size + block_size - 1) / block_size;
		auto count_per_block = total_count / total_blocks;
		auto blocks_per_vector = MaxValue<idx_t>(STANDARD_VECTOR_SIZE / count_per_block, 2);

		// Assume 8 blocks per partition per thread (4 input, 4 output)
		auto partition_multiplier =
		    RadixPartitioning::NumberOfPartitions(sink.hash_table->GetRadixBits() - JoinHashTable::INITIAL_RADIX_BITS);
		auto thread_memory = 2 * blocks_per_vector * partition_multiplier * block_size;
		auto repartition_threads = MaxValue<idx_t>(sink.temporary_memory_state->GetReservation() / thread_memory, 1);

		if (repartition_threads < local_hts.size()) {
			// Limit the number of threads working on repartitioning based on our memory reservation
			for (idx_t thread_idx = repartition_threads; thread_idx < local_hts.size(); thread_idx++) {
				local_hts[thread_idx % repartition_threads]->Merge(*local_hts[thread_idx]);
			}
			local_hts.resize(repartition_threads);
		}

		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> partition_tasks;
		partition_tasks.reserve(local_hts.size());
		for (auto &local_ht : local_hts) {
			partition_tasks.push_back(
			    make_uniq<HashJoinRepartitionTask>(shared_from_this(), context, *sink.hash_table, *local_ht, op));
		}
		SetTasks(std::move(partition_tasks));
	}

	void FinishEvent() override {
		local_hts.clear();

		// Minimum reservation is now the new smallest partition size
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(sink.hash_table->GetRadixBits());
		vector<idx_t> partition_sizes(num_partitions, 0);
		vector<idx_t> partition_counts(num_partitions, 0);
		sink.total_size = sink.hash_table->GetTotalSize(partition_sizes, partition_counts, sink.max_partition_size,
		                                                sink.max_partition_count);
		const auto probe_side_requirement =
		    GetPartitioningSpaceRequirement(sink.context, op.types, sink.hash_table->GetRadixBits(), sink.num_threads);

		sink.temporary_memory_state->SetMinimumReservation(sink.max_partition_size +
		                                                   JoinHashTable::PointerTableSize(sink.max_partition_count) +
		                                                   probe_side_requirement);
		sink.temporary_memory_state->UpdateReservation(executor.context);

		sink.hash_table->PrepareExternalFinalize(sink.temporary_memory_state->GetReservation());
		sink.ScheduleFinalize(*pipeline, *this);
	}
};

void JoinFilterPushdownInfo::PushFilters(JoinFilterGlobalState &gstate, const PhysicalOperator &op) const {
	// finalize the min/max aggregates
	vector<LogicalType> min_max_types;
	for (auto &aggr_expr : min_max_aggregates) {
		min_max_types.push_back(aggr_expr->return_type);
	}
	DataChunk final_min_max;
	final_min_max.Initialize(Allocator::DefaultAllocator(), min_max_types);

	gstate.global_aggregate_state->Finalize(final_min_max);

	// create a filter for each of the aggregates
	for (idx_t filter_idx = 0; filter_idx < filters.size(); filter_idx++) {
		auto &filter = filters[filter_idx];
		auto filter_col_idx = filter.probe_column_index.column_index;
		auto min_idx = filter_idx * 2;
		auto max_idx = min_idx + 1;

		auto min_val = final_min_max.data[min_idx].GetValue(0);
		auto max_val = final_min_max.data[max_idx].GetValue(0);
		if (min_val.IsNull() || max_val.IsNull()) {
			// min/max is NULL
			// this can happen in case all values in the RHS column are NULL, but they are still pushed into the hash
			// table e.g. because they are part of a RIGHT join
			continue;
		}
		if (Value::NotDistinctFrom(min_val, max_val)) {
			// min = max - generate an equality filter
			auto constant_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, std::move(min_val));
			dynamic_filters->PushFilter(op, filter_col_idx, std::move(constant_filter));
		} else {
			// min != max - generate a range filter
			auto greater_equals =
			    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(min_val));
			dynamic_filters->PushFilter(op, filter_col_idx, std::move(greater_equals));
			auto less_equals = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(max_val));
			dynamic_filters->PushFilter(op, filter_col_idx, std::move(less_equals));
		}
		// not null filter
		dynamic_filters->PushFilter(op, filter_col_idx, make_uniq<IsNotNullFilter>());
	}
}

SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &ht = *sink.hash_table;

	sink.temporary_memory_state->UpdateReservation(context);
	sink.external = sink.temporary_memory_state->GetReservation() < sink.total_size;
	if (sink.external) {
		// External Hash Join
		sink.perfect_join_executor.reset();

		const auto max_partition_ht_size =
		    sink.max_partition_size + JoinHashTable::PointerTableSize(sink.max_partition_count);
		if (max_partition_ht_size > sink.temporary_memory_state->GetReservation()) {
			// We have to repartition
			ht.SetRepartitionRadixBits(sink.temporary_memory_state->GetReservation(), sink.max_partition_size,
			                           sink.max_partition_count);
			auto new_event = make_shared_ptr<HashJoinRepartitionEvent>(pipeline, *this, sink, sink.local_hash_tables);
			event.InsertEvent(std::move(new_event));
		} else {
			// No repartitioning! We do need some space for partitioning the probe-side, though
			const auto probe_side_requirement =
			    GetPartitioningSpaceRequirement(context, children[0]->types, ht.GetRadixBits(), sink.num_threads);
			sink.temporary_memory_state->SetMinimumReservation(max_partition_ht_size + probe_side_requirement);
			for (auto &local_ht : sink.local_hash_tables) {
				ht.Merge(*local_ht);
			}
			sink.local_hash_tables.clear();
			sink.hash_table->PrepareExternalFinalize(sink.temporary_memory_state->GetReservation());
			sink.ScheduleFinalize(pipeline, event);
		}
		sink.finalized = true;
		return SinkFinalizeType::READY;
	}

	// In-memory Hash Join
	for (auto &local_ht : sink.local_hash_tables) {
		ht.Merge(*local_ht);
	}
	sink.local_hash_tables.clear();
	ht.Unpartition();

	if (filter_pushdown && ht.Count() > 0) {
		filter_pushdown->PushFilters(*sink.global_filter_state, *this);
	}

	// check for possible perfect hash table
	auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
	if (use_perfect_hash) {
		D_ASSERT(ht.equality_types.size() == 1);
		auto key_type = ht.equality_types[0];
		use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
	}
	// In case of a large build side or duplicates, use regular hash join
	if (!use_perfect_hash) {
		sink.perfect_join_executor.reset();
		sink.ScheduleFinalize(pipeline, event);
	}
	sink.finalized = true;
	if (ht.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class HashJoinOperatorState : public CachingOperatorState {
public:
	explicit HashJoinOperatorState(ClientContext &context, HashJoinGlobalSinkState &sink)
	    : probe_executor(context), scan_structure(*sink.hash_table, join_key_state) {
	}

	DataChunk join_keys;
	TupleDataChunkState join_key_state;

	ExpressionExecutor probe_executor;
	JoinHashTable::ScanStructure scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

	JoinHashTable::ProbeSpillLocalAppendState spill_state;
	JoinHashTable::ProbeState probe_state;
	//! Chunk to sink data into for external join
	DataChunk spill_chunk;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto &allocator = BufferAllocator::Get(context.client);
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto state = make_uniq<HashJoinOperatorState>(context.client, sink);
	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	} else {
		state->join_keys.Initialize(allocator, condition_types);
		for (auto &cond : conditions) {
			state->probe_executor.AddExpression(*cond.left);
		}
		TupleDataCollection::InitializeChunkState(state->join_key_state, condition_types);
	}
	if (sink.external) {
		state->spill_chunk.Initialize(allocator, sink.probe_types);
		sink.InitializeProbeSpill();
	}

	return std::move(state);
}

OperatorResultType PhysicalHashJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<HashJoinOperatorState>();
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	D_ASSERT(sink.finalized);
	D_ASSERT(!sink.scanned_data);

	if (sink.hash_table->Count() == 0) {
		if (EmptyResultIfRHSIsEmpty()) {
			return OperatorResultType::FINISHED;
		}
		ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, input, chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	if (sink.perfect_join_executor) {
		D_ASSERT(!sink.external);
		return sink.perfect_join_executor->ProbePerfectHashTable(context, input, chunk, *state.perfect_hash_join_state);
	}

	if (sink.external && !state.initialized) {
		// some initialization for external hash join
		if (!sink.probe_spill) {
			sink.InitializeProbeSpill();
		}
		state.spill_state = sink.probe_spill->RegisterThread();
		state.initialized = true;
	}

	if (state.scan_structure.is_null) {
		// probe the HT, start by resolving the join keys for the left chunk
		state.join_keys.Reset();
		state.probe_executor.Execute(input, state.join_keys);

		// perform the actual probe
		if (sink.external) {
			sink.hash_table->ProbeAndSpill(state.scan_structure, state.join_keys, state.join_key_state,
			                               state.probe_state, input, *sink.probe_spill, state.spill_state,
			                               state.spill_chunk);
		} else {
			sink.hash_table->Probe(state.scan_structure, state.join_keys, state.join_key_state, state.probe_state);
		}
	}
	state.scan_structure.Next(state.join_keys, input, chunk);

	if (state.scan_structure.PointersExhausted() && chunk.size() == 0) {
		state.scan_structure.is_null = true;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
enum class HashJoinSourceStage : uint8_t { INIT, BUILD, PROBE, SCAN_HT, DONE };

class HashJoinLocalSourceState;

class HashJoinGlobalSourceState : public GlobalSourceState {
public:
	HashJoinGlobalSourceState(const PhysicalHashJoin &op, const ClientContext &context);

	//! Initialize this source state using the info in the sink
	void Initialize(HashJoinGlobalSinkState &sink);
	//! Try to prepare the next stage
	bool TryPrepareNextStage(HashJoinGlobalSinkState &sink);
	//! Prepare the next build/probe/scan_ht stage for external hash join (must hold lock)
	void PrepareBuild(HashJoinGlobalSinkState &sink);
	void PrepareProbe(HashJoinGlobalSinkState &sink);
	void PrepareScanHT(HashJoinGlobalSinkState &sink);
	//! Assigns a task to a local source state
	bool AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate);

	idx_t MaxThreads() override {
		D_ASSERT(op.sink_state);
		auto &gstate = op.sink_state->Cast<HashJoinGlobalSinkState>();

		idx_t count;
		if (gstate.probe_spill) {
			count = probe_count;
		} else if (PropagatesBuildSide(op.join_type)) {
			count = gstate.hash_table->Count();
		} else {
			return 0;
		}
		return count / ((idx_t)STANDARD_VECTOR_SIZE * parallel_scan_chunk_count);
	}

public:
	const PhysicalHashJoin &op;

	//! For synchronizing the external hash join
	atomic<HashJoinSourceStage> global_stage;

	//! For HT build synchronization
	idx_t build_chunk_idx = DConstants::INVALID_INDEX;
	idx_t build_chunk_count;
	idx_t build_chunk_done;
	idx_t build_chunks_per_thread = DConstants::INVALID_INDEX;

	//! For probe synchronization
	atomic<idx_t> probe_chunk_count;
	idx_t probe_chunk_done;

	//! To determine the number of threads
	idx_t probe_count;
	idx_t parallel_scan_chunk_count;

	//! For full/outer synchronization
	idx_t full_outer_chunk_idx = DConstants::INVALID_INDEX;
	atomic<idx_t> full_outer_chunk_count;
	atomic<idx_t> full_outer_chunk_done;
	idx_t full_outer_chunks_per_thread = DConstants::INVALID_INDEX;

	vector<InterruptState> blocked_tasks;
};

class HashJoinLocalSourceState : public LocalSourceState {
public:
	HashJoinLocalSourceState(const PhysicalHashJoin &op, const HashJoinGlobalSinkState &sink, Allocator &allocator);

	//! Do the work this thread has been assigned
	void ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished() const;
	//! Build, probe and scan for external hash join
	void ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate);
	void ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	void ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);

public:
	//! The stage that this thread was assigned work for
	HashJoinSourceStage local_stage;
	//! Vector with pointers here so we don't have to re-initialize
	Vector addresses;

	//! Chunks assigned to this thread for building the pointer table
	idx_t build_chunk_idx_from = DConstants::INVALID_INDEX;
	idx_t build_chunk_idx_to = DConstants::INVALID_INDEX;

	//! Local scan state for probe spill
	ColumnDataConsumerScanState probe_local_scan;
	//! Chunks for holding the scanned probe collection
	DataChunk probe_chunk;
	DataChunk join_keys;
	DataChunk payload;
	TupleDataChunkState join_key_state;

	//! Column indices to easily reference the join keys/payload columns in probe_chunk
	vector<idx_t> join_key_indices;
	vector<idx_t> payload_indices;
	//! Scan structure for the external probe
	JoinHashTable::ScanStructure scan_structure;
	JoinHashTable::ProbeState probe_state;
	bool empty_ht_probe_in_progress = false;

	//! Chunks assigned to this thread for a full/outer scan
	idx_t full_outer_chunk_idx_from = DConstants::INVALID_INDEX;
	idx_t full_outer_chunk_idx_to = DConstants::INVALID_INDEX;
	unique_ptr<JoinHTScanState> full_outer_scan_state;
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<HashJoinGlobalSourceState>(*this, context);
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<HashJoinLocalSourceState>(*this, sink_state->Cast<HashJoinGlobalSinkState>(),
	                                           BufferAllocator::Get(context.client));
}

HashJoinGlobalSourceState::HashJoinGlobalSourceState(const PhysicalHashJoin &op, const ClientContext &context)
    : op(op), global_stage(HashJoinSourceStage::INIT), build_chunk_count(0), build_chunk_done(0), probe_chunk_count(0),
      probe_chunk_done(0), probe_count(op.children[0]->estimated_cardinality),
      parallel_scan_chunk_count(context.config.verify_parallelism ? 1 : 120) {
}

void HashJoinGlobalSourceState::Initialize(HashJoinGlobalSinkState &sink) {
	auto guard = Lock();
	if (global_stage != HashJoinSourceStage::INIT) {
		// Another thread initialized
		return;
	}

	// Finalize the probe spill
	if (sink.probe_spill) {
		sink.probe_spill->Finalize();
	}

	global_stage = HashJoinSourceStage::PROBE;
	TryPrepareNextStage(sink);
}

bool HashJoinGlobalSourceState::TryPrepareNextStage(HashJoinGlobalSinkState &sink) {
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_chunk_done == build_chunk_count) {
			sink.hash_table->GetDataCollection().VerifyEverythingPinned();
			sink.hash_table->finalized = true;
			PrepareProbe(sink);
			return true;
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (probe_chunk_done == probe_chunk_count) {
			if (PropagatesBuildSide(op.join_type)) {
				PrepareScanHT(sink);
			} else {
				PrepareBuild(sink);
			}
			return true;
		}
		break;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_chunk_done == full_outer_chunk_count) {
			PrepareBuild(sink);
			return true;
		}
		break;
	default:
		break;
	}
	return false;
}

void HashJoinGlobalSourceState::PrepareBuild(HashJoinGlobalSinkState &sink) {
	D_ASSERT(global_stage != HashJoinSourceStage::BUILD);
	auto &ht = *sink.hash_table;

	// Update remaining size
	sink.temporary_memory_state->SetRemainingSizeAndUpdateReservation(sink.context, ht.GetRemainingSize());

	// Try to put the next partitions in the block collection of the HT
	if (!sink.external || !ht.PrepareExternalFinalize(sink.temporary_memory_state->GetReservation())) {
		global_stage = HashJoinSourceStage::DONE;
		sink.temporary_memory_state->SetZero();
		return;
	}

	auto &data_collection = ht.GetDataCollection();
	if (data_collection.Count() == 0 && op.EmptyResultIfRHSIsEmpty()) {
		PrepareBuild(sink);
		return;
	}

	build_chunk_idx = 0;
	build_chunk_count = data_collection.ChunkCount();
	build_chunk_done = 0;

	build_chunks_per_thread = MaxValue<idx_t>((build_chunk_count + sink.num_threads - 1) / sink.num_threads, 1);

	ht.InitializePointerTable();

	global_stage = HashJoinSourceStage::BUILD;
}

void HashJoinGlobalSourceState::PrepareProbe(HashJoinGlobalSinkState &sink) {
	sink.probe_spill->PrepareNextProbe();
	const auto &consumer = *sink.probe_spill->consumer;

	probe_chunk_count = consumer.Count() == 0 ? 0 : consumer.ChunkCount();
	probe_chunk_done = 0;

	global_stage = HashJoinSourceStage::PROBE;
	if (probe_chunk_count == 0) {
		TryPrepareNextStage(sink);
		return;
	}
}

void HashJoinGlobalSourceState::PrepareScanHT(HashJoinGlobalSinkState &sink) {
	D_ASSERT(global_stage != HashJoinSourceStage::SCAN_HT);
	auto &ht = *sink.hash_table;

	auto &data_collection = ht.GetDataCollection();
	full_outer_chunk_idx = 0;
	full_outer_chunk_count = data_collection.ChunkCount();
	full_outer_chunk_done = 0;

	full_outer_chunks_per_thread =
	    MaxValue<idx_t>((full_outer_chunk_count + sink.num_threads - 1) / sink.num_threads, 1);

	global_stage = HashJoinSourceStage::SCAN_HT;
}

bool HashJoinGlobalSourceState::AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate) {
	D_ASSERT(lstate.TaskFinished());

	auto guard = Lock();
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_chunk_idx != build_chunk_count) {
			lstate.local_stage = global_stage;
			lstate.build_chunk_idx_from = build_chunk_idx;
			build_chunk_idx = MinValue<idx_t>(build_chunk_count, build_chunk_idx + build_chunks_per_thread);
			lstate.build_chunk_idx_to = build_chunk_idx;
			return true;
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (sink.probe_spill->consumer && sink.probe_spill->consumer->AssignChunk(lstate.probe_local_scan)) {
			lstate.local_stage = global_stage;
			lstate.empty_ht_probe_in_progress = false;
			return true;
		}
		break;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_chunk_idx != full_outer_chunk_count) {
			lstate.local_stage = global_stage;
			lstate.full_outer_chunk_idx_from = full_outer_chunk_idx;
			full_outer_chunk_idx =
			    MinValue<idx_t>(full_outer_chunk_count, full_outer_chunk_idx + full_outer_chunks_per_thread);
			lstate.full_outer_chunk_idx_to = full_outer_chunk_idx;
			return true;
		}
		break;
	case HashJoinSourceStage::DONE:
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in AssignTask!");
	}
	return false;
}

HashJoinLocalSourceState::HashJoinLocalSourceState(const PhysicalHashJoin &op, const HashJoinGlobalSinkState &sink,
                                                   Allocator &allocator)
    : local_stage(HashJoinSourceStage::INIT), addresses(LogicalType::POINTER),
      scan_structure(*sink.hash_table, join_key_state) {
	auto &chunk_state = probe_local_scan.current_chunk_state;
	chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;

	probe_chunk.Initialize(allocator, sink.probe_types);
	join_keys.Initialize(allocator, op.condition_types);
	payload.Initialize(allocator, op.children[0]->types);
	TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);

	// Store the indices of the columns to reference them easily
	idx_t col_idx = 0;
	for (; col_idx < op.condition_types.size(); col_idx++) {
		join_key_indices.push_back(col_idx);
	}
	for (; col_idx < sink.probe_types.size() - 1; col_idx++) {
		payload_indices.push_back(col_idx);
	}
}

void HashJoinLocalSourceState::ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                           DataChunk &chunk) {
	switch (local_stage) {
	case HashJoinSourceStage::BUILD:
		ExternalBuild(sink, gstate);
		break;
	case HashJoinSourceStage::PROBE:
		ExternalProbe(sink, gstate, chunk);
		break;
	case HashJoinSourceStage::SCAN_HT:
		ExternalScanHT(sink, gstate, chunk);
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in ExecuteTask!");
	}
}

bool HashJoinLocalSourceState::TaskFinished() const {
	switch (local_stage) {
	case HashJoinSourceStage::INIT:
	case HashJoinSourceStage::BUILD:
		return true;
	case HashJoinSourceStage::PROBE:
		return scan_structure.is_null && !empty_ht_probe_in_progress;
	case HashJoinSourceStage::SCAN_HT:
		return full_outer_scan_state == nullptr;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in TaskFinished!");
	}
}

void HashJoinLocalSourceState::ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate) {
	D_ASSERT(local_stage == HashJoinSourceStage::BUILD);

	auto &ht = *sink.hash_table;
	ht.Finalize(build_chunk_idx_from, build_chunk_idx_to, true);

	auto guard = gstate.Lock();
	gstate.build_chunk_done += build_chunk_idx_to - build_chunk_idx_from;
}

void HashJoinLocalSourceState::ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                             DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::PROBE && sink.hash_table->finalized);

	if (!scan_structure.is_null) {
		// Still have elements remaining (i.e. we got >STANDARD_VECTOR_SIZE elements in the previous probe)
		scan_structure.Next(join_keys, payload, chunk);
		if (chunk.size() != 0 || !scan_structure.PointersExhausted()) {
			return;
		}
	}

	if (!scan_structure.is_null || empty_ht_probe_in_progress) {
		// Previous probe is done
		scan_structure.is_null = true;
		empty_ht_probe_in_progress = false;
		sink.probe_spill->consumer->FinishChunk(probe_local_scan);
		auto guard = gstate.Lock();
		gstate.probe_chunk_done++;
		return;
	}

	// Scan input chunk for next probe
	sink.probe_spill->consumer->ScanChunk(probe_local_scan, probe_chunk);

	// Get the probe chunk columns/hashes
	join_keys.ReferenceColumns(probe_chunk, join_key_indices);
	payload.ReferenceColumns(probe_chunk, payload_indices);
	auto precomputed_hashes = &probe_chunk.data.back();

	if (sink.hash_table->Count() == 0 && !gstate.op.EmptyResultIfRHSIsEmpty()) {
		gstate.op.ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, payload, chunk);
		empty_ht_probe_in_progress = true;
		return;
	}

	// Perform the probe
	sink.hash_table->Probe(scan_structure, join_keys, join_key_state, probe_state, precomputed_hashes);
	scan_structure.Next(join_keys, payload, chunk);
}

void HashJoinLocalSourceState::ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                              DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::SCAN_HT);

	if (!full_outer_scan_state) {
		full_outer_scan_state = make_uniq<JoinHTScanState>(sink.hash_table->GetDataCollection(),
		                                                   full_outer_chunk_idx_from, full_outer_chunk_idx_to);
	}
	sink.hash_table->ScanFullOuter(*full_outer_scan_state, addresses, chunk);

	if (chunk.size() == 0) {
		full_outer_scan_state = nullptr;
		auto guard = gstate.Lock();
		gstate.full_outer_chunk_done += full_outer_chunk_idx_to - full_outer_chunk_idx_from;
	}
}

SourceResultType PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto &gstate = input.global_state.Cast<HashJoinGlobalSourceState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSourceState>();
	sink.scanned_data = true;

	if (!sink.external && !PropagatesBuildSide(join_type)) {
		auto guard = gstate.Lock();
		if (gstate.global_stage != HashJoinSourceStage::DONE) {
			gstate.global_stage = HashJoinSourceStage::DONE;
			sink.hash_table->Reset();
			sink.temporary_memory_state->SetZero();
		}
		return SourceResultType::FINISHED;
	}

	if (gstate.global_stage == HashJoinSourceStage::INIT) {
		gstate.Initialize(sink);
	}

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	while (gstate.global_stage != HashJoinSourceStage::DONE && chunk.size() == 0) {
		if (!lstate.TaskFinished() || gstate.AssignTask(sink, lstate)) {
			lstate.ExecuteTask(sink, gstate, chunk);
		} else {
			auto guard = gstate.Lock();
			if (gstate.TryPrepareNextStage(sink) || gstate.global_stage == HashJoinSourceStage::DONE) {
				gstate.UnblockTasks(guard);
			} else {
				return gstate.BlockSource(guard, input.interrupt_state);
			}
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

double PhysicalHashJoin::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto &gstate = gstate_p.Cast<HashJoinGlobalSourceState>();

	if (!sink.external) {
		if (PropagatesBuildSide(join_type)) {
			return static_cast<double>(gstate.full_outer_chunk_done) /
			       static_cast<double>(gstate.full_outer_chunk_count) * 100.0;
		}
		return 100.0;
	}

	auto num_partitions = static_cast<double>(RadixPartitioning::NumberOfPartitions(sink.hash_table->GetRadixBits()));
	auto partition_start = static_cast<double>(sink.hash_table->GetPartitionStart());
	auto partition_end = static_cast<double>(sink.hash_table->GetPartitionEnd());

	// This many partitions are fully done
	auto progress = partition_start / num_partitions;

	auto probe_chunk_done = static_cast<double>(gstate.probe_chunk_done);
	auto probe_chunk_count = static_cast<double>(gstate.probe_chunk_count);
	if (probe_chunk_count != 0) {
		// Progress of the current round of probing, weighed by the number of partitions
		auto probe_progress = probe_chunk_done / probe_chunk_count;
		// Add it to the progress, weighed by the number of partitions in the current round
		progress += (partition_end - partition_start) / num_partitions * probe_progress;
	}

	return progress * 100.0;
}

InsertionOrderPreservingMap<string> PhysicalHashJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToString(join_type);

	string condition_info;
	for (idx_t i = 0; i < conditions.size(); i++) {
		auto &join_condition = conditions[i];
		if (i > 0) {
			condition_info += "\n";
		}
		condition_info +=
		    StringUtil::Format("%s %s %s", join_condition.left->GetName(),
		                       ExpressionTypeToOperator(join_condition.comparison), join_condition.right->GetName());
	}
	result["Conditions"] = condition_info;

	if (perfect_join_statistics.is_build_small) {
		// perfect hash join
		result["Build Min"] = perfect_join_statistics.build_min.ToString();
		result["Build Max"] = perfect_join_statistics.build_max.ToString();
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
