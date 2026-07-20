#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/main/settings.hpp"

namespace duckdb {

struct RecursiveCTEDistinctPartition {
	RecursiveCTEDistinctPartition(ClientContext &context, const vector<LogicalType> &types)
	    : ht(context, BufferAllocator::Get(context), types) {
	}

	mutex lock;
	GroupedAggregateHashTable ht;
};

PhysicalRecursiveCTE::PhysicalRecursiveCTE(PhysicalPlan &physical_plan, Identifier ctename, TableIndex table_index,
                                           vector<LogicalType> types, bool union_all, PhysicalOperator &top,
                                           PhysicalOperator &bottom, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RECURSIVE_CTE, std::move(types), estimated_cardinality),
      ctename(std::move(ctename)), table_index(table_index), union_all(union_all),
      shared_executor_pool(make_shared_ptr<RecursiveExecutorPool>()) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

//===--------------------------------------------------------------------===//
// Sink State
//===--------------------------------------------------------------------===//
RecursiveCTEState::RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
    : op(op), executor(context), allow_executor_reuse(Settings::Get<EnableCachingOperatorsSetting>(context)),
      runtime_logger(context.logger),
      collect_runtime_metrics(runtime_logger &&
                              runtime_logger->ShouldLog(PhysicalOperatorLogType::NAME, PhysicalOperatorLogType::LEVEL)),
      executor_pool(op.shared_executor_pool),
      intermediate_table(context, op.using_key ? op.internal_types : op.GetTypes()) {
	vector<LogicalType> aggr_input_types;
	vector<AggregateObject> payload_aggregates;
	for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
		D_ASSERT(op.payload_aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &bound_aggr_expr = op.payload_aggregates[i]->Cast<BoundAggregateExpression>();
		for (auto &child_expr : bound_aggr_expr.GetChildren()) {
			executor.AddExpression(*child_expr);
			aggr_input_types.push_back(child_expr->GetReturnType());
		}
		payload_aggregates.emplace_back(bound_aggr_expr);
	}

	payload_rows.Initialize(Allocator::Get(context), aggr_input_types);

	if (op.using_key) {
		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types,
		                                          op.payload_types, std::move(payload_aggregates));
	} else if (!op.union_all) {
		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types);
	}
	if (op.using_key) {
		distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
		update_rows.Initialize(Allocator::DefaultAllocator(), op.internal_types);
		source_distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
		source_payload_rows.Initialize(Allocator::DefaultAllocator(), op.payload_types);
	}
	source_result.Initialize(Allocator::DefaultAllocator(), op.GetTypes());
	if (op.using_key) {
		InitializeIntermediateAppend();
		op.working_table->InitializeAppend(working_append_state);
	}
	if (op.recurring_table) {
		op.recurring_table->InitializeAppend(recurring_append_state);
	}
}

RecursiveCTEState::~RecursiveCTEState() {
	if (collect_runtime_metrics) {
		DUCKDB_LOG(runtime_logger, PhysicalOperatorLogType, op, "PhysicalRecursiveCTE", "RuntimeMetrics",
		           {{"epochs", to_string(cumulative_epoch_count)},
		            {"scheduled_workers", to_string(cumulative_worker_count)},
		            {"scheduled_tasks", to_string(cumulative_task_count.load())},
		            {"elapsed_us", to_string(cumulative_elapsed_us)},
		            {"frontier_rows", to_string(cumulative_frontier_rows)},
		            {"frontier_chunks", to_string(cumulative_frontier_chunks)},
		            {"frontier_storage_bytes", to_string(cumulative_frontier_storage_bytes)},
		            {"sink_lock_wait_ns", to_string(cumulative_sink_wait_ns.load())},
		            {"sink_lock_work_ns", to_string(cumulative_sink_work_ns.load())},
		            {"sink_lock_rows", to_string(cumulative_sink_rows.load())},
		            {"sink_lock_calls", to_string(cumulative_sink_calls.load())},
		            {"hash_rows", to_string(cumulative_hash_rows.load())},
		            {"recurring_scan_rows", to_string(cumulative_recurring_scan_rows.load())},
		            {"final_state_rows", to_string(cumulative_final_state_rows)},
		            {"retained_build_executions", to_string(retained_build_executions)}});
	}
	ClearCachedExecutors();
}

void RecursiveCTEState::RecordSinkMetrics(idx_t wait_ns, idx_t work_ns, idx_t rows) {
	cumulative_sink_wait_ns.fetch_add(wait_ns);
	cumulative_sink_work_ns.fetch_add(work_ns);
	cumulative_sink_rows.fetch_add(rows);
	cumulative_sink_calls.fetch_add(1);
}

void RecursiveCTEState::LogThreadLimitChanged(idx_t previous_limit, idx_t new_limit, idx_t elapsed_us, idx_t work_units,
                                              idx_t frontier_rows) {
	if (!collect_runtime_metrics) {
		return;
	}
	DUCKDB_LOG(runtime_logger, PhysicalOperatorLogType, op, "PhysicalRecursiveCTE", "ThreadLimitChanged",
	           {{"previous_limit", to_string(previous_limit)},
	            {"new_limit", to_string(new_limit)},
	            {"elapsed_us", to_string(elapsed_us)},
	            {"work_units", to_string(work_units)},
	            {"frontier_rows", to_string(frontier_rows)}});
}

void RecursiveCTEState::InitializeIntermediateAppend() {
	intermediate_table.InitializeAppend(intermediate_append_state);
}

void RecursiveCTEState::ResetRecurringTable() {
	D_ASSERT(op.recurring_table);
	op.recurring_table->Reset();
	op.recurring_table->InitializeAppend(recurring_append_state);
}

ColumnDataCollection &RecursiveCTEState::CurrentOutputTable() {
	if (op.using_key || !output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

ColumnDataCollection &RecursiveCTEState::CurrentInputTable() {
	if (op.using_key) {
		D_ASSERT(op.working_table);
		return *op.working_table;
	}
	if (output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

const ColumnDataCollection &RecursiveCTEState::CurrentInputTable() const {
	if (op.using_key) {
		D_ASSERT(op.working_table);
		return *op.working_table;
	}
	if (output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

ColumnDataAppendState &RecursiveCTEState::CurrentOutputAppendState() {
	if (op.using_key || !output_is_working) {
		return intermediate_append_state;
	}
	return working_append_state;
}

void RecursiveCTEState::AdvanceIterationBuffers() {
	if (!op.using_key) {
		output_is_working = !output_is_working;
	}
}

void RecursiveCTEState::ResetCurrentOutputTableForReuse() {
	auto &output = CurrentOutputTable();
	output.ResetForReuse();
	if (!op.using_key) {
		return;
	}
	if (op.using_key || !output_is_working) {
		InitializeIntermediateAppend();
	} else {
		D_ASSERT(op.working_table);
		op.working_table->InitializeAppend(working_append_state);
	}
}

void RecursiveCTEState::RebindRecursiveScans() {
	if (op.using_key) {
		return;
	}
	auto &input_table = CurrentInputTable();
	for (auto &scan_ref : op.recursive_scans) {
		auto &scan = scan_ref.get();
		scan.collection = input_table;
	}
}

void RecursiveCTEState::PrepareCachedExecutorEntry(Pipeline &pipeline) {
	cached_executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>());
}

void RecursiveCTEState::PrepareCachedExecutors(Pipeline &pipeline, idx_t max_threads) {
	// Ordinary pipelines build PipelineExecutors once and discard them after the query finishes.
	// Recursive CTEs re-enter the same pipelines many times, and correlated recursive invocations can
	// construct several RecursiveCTEState objects for the same physical plan. Keep a state-local cache
	// for cheap per-iteration reuse, and spill back into a shared pool so later states can recycle the
	// already-initialized executors instead of reconstructing them from scratch.
	auto entry = cached_executors.find(pipeline);
	D_ASSERT(entry != cached_executors.end());
	auto &executors = entry->second;
	if (executors.size() >= max_threads) {
		return;
	}
	if (!allow_executor_reuse) {
		while (executors.size() < max_threads) {
			executors.push_back(make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline));
		}
		return;
	}
	D_ASSERT(executor_pool);
	lock_guard<mutex> pool_guard(executor_pool->lock);
	auto pool_entry = executor_pool->executors.find(pipeline);
	if (pool_entry == executor_pool->executors.end()) {
		pool_entry =
		    executor_pool->executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>())
		        .first;
	}
	auto &shared_executors = pool_entry->second;
	while (executors.size() < max_threads) {
		if (!shared_executors.empty()) {
			executors.push_back(std::move(shared_executors.back()));
			shared_executors.pop_back();
		} else {
			executors.push_back(make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline));
		}
	}
}

vector<unique_ptr<PipelineExecutor>> &RecursiveCTEState::GetCachedExecutors(Pipeline &pipeline) {
	auto entry = cached_executors.find(pipeline);
	D_ASSERT(entry != cached_executors.end());
	return entry->second;
}

void RecursiveCTEState::ClearCachedExecutors() {
	if (cached_executors.empty()) {
		return;
	}
	if (!allow_executor_reuse) {
		cached_executors.clear();
		return;
	}
	D_ASSERT(executor_pool);
	lock_guard<mutex> pool_guard(executor_pool->lock);
	for (auto &entry : cached_executors) {
		auto pool_entry = executor_pool->executors.find(entry.first.get());
		if (pool_entry == executor_pool->executors.end()) {
			pool_entry = executor_pool->executors.emplace(entry.first, vector<unique_ptr<PipelineExecutor>>()).first;
		}
		auto &shared_executors = pool_entry->second;
		for (auto &executor : entry.second) {
			shared_executors.push_back(std::move(executor));
		}
	}
	cached_executors.clear();
}

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveCTEState>(context, *this);
}

class RecursiveCTELocalState : public LocalSinkState {
public:
	RecursiveCTELocalState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : hashes(LogicalType::HASH), partition_hashes(LogicalType::HASH), dummy_addresses(LogicalType::POINTER),
	      new_groups(STANDARD_VECTOR_SIZE) {
		if (!op.using_key) {
			output = make_uniq<ColumnDataCollection>(context, op.GetTypes());
			output->InitializeAppend(append_state);
		}
		if (!op.using_key && !op.union_all) {
			partition_chunk.Initialize(Allocator::Get(context), op.GetTypes());
		}
	}

	unique_ptr<ColumnDataCollection> output;
	ColumnDataAppendState append_state;
	Vector hashes;
	Vector partition_hashes;
	Vector dummy_addresses;
	SelectionVector new_groups;
	DataChunk partition_chunk;
	vector<SelectionVector> partition_selections;
	vector<idx_t> partition_counts;

	void InitializePartitions(idx_t partition_count) {
		if (partition_selections.size() == partition_count) {
			return;
		}
		partition_selections.clear();
		partition_selections.reserve(partition_count);
		for (idx_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
			partition_selections.emplace_back(STANDARD_VECTOR_SIZE);
		}
		partition_counts.resize(partition_count);
	}

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSinkState &gstate) override {
		if (output) {
			output->ResetForReuse();
			output->InitializeAppend(append_state);
		}
	}
};

unique_ptr<LocalSinkState> PhysicalRecursiveCTE::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RecursiveCTELocalState>(context.client, *this);
}

static void SinkSerialDistinctChunk(DataChunk &chunk, RecursiveCTEState &gstate, RecursiveCTELocalState &lstate) {
	D_ASSERT(gstate.ht);
	const auto before_lock =
	    gstate.collect_runtime_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	idx_t new_group_count;
	{
		lock_guard<mutex> guard(gstate.intermediate_table_lock);
		const auto after_lock =
		    gstate.collect_runtime_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		if (gstate.collect_runtime_metrics) {
			gstate.cumulative_hash_rows.fetch_add(chunk.size());
		}
		new_group_count = gstate.ht->FindOrCreateGroups(chunk, lstate.dummy_addresses, lstate.new_groups);
		chunk.Slice(lstate.new_groups, new_group_count);
		if (gstate.collect_runtime_metrics) {
			const auto after_work = std::chrono::steady_clock::now();
			gstate.RecordSinkMetrics(
			    NumericCast<idx_t>(
			        std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
			    NumericCast<idx_t>(
			        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
			    chunk.size());
		}
	}
	if (new_group_count > 0) {
		lstate.output->Append(lstate.append_state, chunk);
	}
}

static void SinkDistinctChunk(DataChunk &chunk, RecursiveCTEState &gstate, RecursiveCTELocalState &lstate,
                              bool emit_rows = true, bool record_sink_metrics = true) {
	auto &partitions = gstate.distinct_partitions;
	D_ASSERT(!partitions.empty());
	D_ASSERT((partitions.size() & (partitions.size() - 1)) == 0);
	lstate.InitializePartitions(partitions.size());
	std::fill(lstate.partition_counts.begin(), lstate.partition_counts.end(), 0);

	chunk.Hash(lstate.hashes);
	auto hash_data = FlatVector::GetData<hash_t>(lstate.hashes);
	const auto partition_mask = partitions.size() - 1;
	for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		const auto partition_idx = hash_data[row_idx] & partition_mask;
		auto &partition_count = lstate.partition_counts[partition_idx];
		lstate.partition_selections[partition_idx].set_index(partition_count++, row_idx);
	}

	for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
		const auto partition_count = lstate.partition_counts[partition_idx];
		if (partition_count == 0) {
			continue;
		}
		lstate.partition_chunk.Reset();
		lstate.partition_chunk.Slice(chunk, lstate.partition_selections[partition_idx], partition_count);
		lstate.partition_hashes.Slice(lstate.hashes, lstate.partition_selections[partition_idx], partition_count);
		auto &partition = *partitions[partition_idx];
		const auto collect_sink_metrics = gstate.collect_runtime_metrics && record_sink_metrics;
		const auto before_lock =
		    collect_sink_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		idx_t new_group_count;
		{
			lock_guard<mutex> guard(partition.lock);
			const auto after_lock =
			    collect_sink_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			if (gstate.collect_runtime_metrics) {
				gstate.cumulative_hash_rows.fetch_add(partition_count);
			}
			new_group_count = partition.ht.FindOrCreateGroups(lstate.partition_chunk, lstate.partition_hashes,
			                                                  lstate.dummy_addresses, lstate.new_groups);
			lstate.partition_chunk.Slice(lstate.new_groups, new_group_count);
			if (collect_sink_metrics) {
				const auto after_work = std::chrono::steady_clock::now();
				gstate.RecordSinkMetrics(
				    NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
				    NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
				    partition_count);
			}
		}
		if (emit_rows && new_group_count > 0) {
			lstate.output->Append(lstate.append_state, lstate.partition_chunk);
		}
	}
}

void RecursiveCTEState::PromoteDistinctState(ClientContext &context, idx_t partition_count) {
	D_ASSERT(!op.using_key && !op.union_all);
	if (!distinct_partitions.empty() || partition_count <= 1) {
		return;
	}
	D_ASSERT(ht);
	const auto migrated_rows = ht->Count();
	const auto promotion_start =
	    collect_runtime_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	distinct_partitions.reserve(partition_count);
	for (idx_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
		distinct_partitions.push_back(make_uniq<RecursiveCTEDistinctPartition>(context, op.distinct_types));
	}

	RecursiveCTELocalState migration_state(context, op);
	DataChunk groups;
	groups.Initialize(Allocator::Get(context), op.distinct_types);
	DataChunk payload;
	AggregateHTScanState scan_state;
	ht->InitializeScan(scan_state);
	while (ht->Scan(scan_state, groups, payload)) {
		context.InterruptCheck();
		if (groups.size() > 0) {
			SinkDistinctChunk(groups, *this, migration_state, false, false);
		}
	}
	ht.reset();
	if (collect_runtime_metrics) {
		const auto promotion_end = std::chrono::steady_clock::now();
		const auto elapsed_us = NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::microseconds>(promotion_end - promotion_start).count());
		DUCKDB_LOG(runtime_logger, PhysicalOperatorLogType, op, "PhysicalRecursiveCTE", "DistinctPromoted",
		           {{"partitions", to_string(partition_count)},
		            {"migrated_rows", to_string(migrated_rows)},
		            {"elapsed_us", to_string(elapsed_us)}});
	}
}

static void GatherChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
	}
}

static void ScatterChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
	}
}

void RecursiveCTEState::CommitUsingKeyUpdates() {
	D_ASSERT(op.using_key);
	ColumnDataScanState update_scan_state;
	intermediate_table.InitializeScan(update_scan_state);
	while (intermediate_table.Scan(update_scan_state, update_rows)) {
		if (collect_runtime_metrics) {
			cumulative_hash_rows.fetch_add(update_rows.size());
		}
		distinct_rows.Reset();
		GatherChunk(distinct_rows, update_rows, op.distinct_idx);
		if (!executor.expressions.empty()) {
			payload_rows.Reset();
			executor.Execute(update_rows, payload_rows);
		}
		ht->AddChunk(distinct_rows, payload_rows, AggregateType::NON_DISTINCT);
	}
}

class RecursiveCTEStateScanGlobalState : public GlobalSourceState {
public:
	mutex lock;
	AggregateHTScanState scan_state;
	bool initialized = false;
};

class RecursiveCTEStateScanLocalState : public LocalSourceState {
public:
	RecursiveCTEStateScanLocalState(ClientContext &context, const PhysicalRecursiveCTE &op) {
		distinct_rows.Initialize(Allocator::Get(context), op.distinct_types);
		payload_rows.Initialize(Allocator::Get(context), op.payload_types);
	}

	DataChunk distinct_rows;
	DataChunk payload_rows;
};

PhysicalRecursiveCTEStateScan::PhysicalRecursiveCTEStateScan(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                             idx_t estimated_cardinality, TableIndex cte_index)
    : PhysicalColumnDataScan(physical_plan, std::move(types), PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN,
                             estimated_cardinality, cte_index) {
}

unique_ptr<GlobalSourceState> PhysicalRecursiveCTEStateScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RecursiveCTEStateScanGlobalState>();
}

unique_ptr<LocalSourceState> PhysicalRecursiveCTEStateScan::GetLocalSourceState(ExecutionContext &context,
                                                                                GlobalSourceState &gstate) const {
	if (!recursive_cte) {
		throw InternalException("USING KEY state scan is not linked to its recursive CTE");
	}
	return make_uniq<RecursiveCTEStateScanLocalState>(context.client, *recursive_cte);
}

SourceResultType PhysicalRecursiveCTEStateScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                OperatorSourceInput &input) const {
	if (!recursive_cte || !recursive_cte->sink_state) {
		throw InternalException("USING KEY state scan has no recursive state");
	}
	auto &recursive_state = recursive_cte->sink_state->Cast<RecursiveCTEState>();
	auto &gstate = input.global_state.Cast<RecursiveCTEStateScanGlobalState>();
	auto &lstate = input.local_state.Cast<RecursiveCTEStateScanLocalState>();
	lock_guard<mutex> guard(gstate.lock);
	if (!gstate.initialized) {
		recursive_state.ht->InitializeScan(gstate.scan_state);
		gstate.initialized = true;
	}
	while (recursive_state.ht->Scan(gstate.scan_state, lstate.distinct_rows, lstate.payload_rows)) {
		if (lstate.distinct_rows.size() == 0) {
			continue;
		}
		ScatterChunk(chunk, lstate.distinct_rows, distinct_idx);
		ScatterChunk(chunk, lstate.payload_rows, payload_idx);
		chunk.SetCardinalityUnsafe(lstate.distinct_rows.size());
		if (recursive_state.collect_runtime_metrics) {
			recursive_state.cumulative_recurring_scan_rows.fetch_add(chunk.size());
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return SourceResultType::FINISHED;
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTEStateScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Index"] = StringUtil::Format("%llu", cte_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();

	if (!using_key && union_all) {
		auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
		D_ASSERT(lstate.output);
		lstate.output->Append(lstate.append_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (!using_key) {
		auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
		D_ASSERT(lstate.output);
		if (gstate.distinct_partitions.empty()) {
			SinkSerialDistinctChunk(chunk, gstate, lstate);
		} else {
			SinkDistinctChunk(chunk, gstate, lstate);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	const auto before_lock =
	    gstate.collect_runtime_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	{
		lock_guard<mutex> guard(gstate.intermediate_table_lock);
		const auto after_lock =
		    gstate.collect_runtime_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		// Collect updates without mutating the hash state read by recurring.T in this epoch.
		gstate.intermediate_table.Append(gstate.intermediate_append_state, chunk);
		if (gstate.collect_runtime_metrics) {
			const auto after_work = std::chrono::steady_clock::now();
			gstate.RecordSinkMetrics(
			    NumericCast<idx_t>(
			        std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
			    NumericCast<idx_t>(
			        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
			    chunk.size());
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalRecursiveCTE::PrepareFinalize(ClientContext &context, GlobalSinkState &sink_state) const {
	if (using_key) {
		sink_state.Cast<RecursiveCTEState>().CommitUsingKeyUpdates();
	}
}

SinkCombineResultType PhysicalRecursiveCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	if (!using_key) {
		auto &gstate = input.global_state.Cast<RecursiveCTEState>();
		auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
		D_ASSERT(lstate.output);
		const auto before_lock =
		    gstate.collect_runtime_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		{
			lock_guard<mutex> guard(gstate.intermediate_table_lock);
			const auto after_lock = gstate.collect_runtime_metrics ? std::chrono::steady_clock::now()
			                                                       : std::chrono::steady_clock::time_point();
			const auto row_count = lstate.output->Count();
			gstate.CurrentOutputTable().Combine(*lstate.output);
			if (gstate.collect_runtime_metrics) {
				const auto after_work = std::chrono::steady_clock::now();
				gstate.RecordSinkMetrics(
				    NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
				    NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
				    row_count);
			}
		}
	}
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRecursiveCTE::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	if (!gstate.initialized) {
		if (!using_key) {
			gstate.CurrentOutputTable().InitializeScan(gstate.scan_state);
		}
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	if (using_key) {
		while (true) {
			switch (gstate.key_source_phase) {
			case RecursiveCTEKeySourcePhase::RECURSING: {
				const auto expected_new = gstate.intermediate_table.Count();
				working_table->Reset();
				working_table->Combine(gstate.intermediate_table);
				gstate.InitializeIntermediateAppend();

				if (expected_new > 0) {
					const auto desired_capacity =
					    GroupedAggregateHashTable::GetCapacityForCount(gstate.ht->Count() + expected_new);
					if (desired_capacity > gstate.ht->Capacity()) {
						gstate.ht->Resize(desired_capacity);
					}
				}

				ExecuteRecursivePipelines(context);
				if (gstate.intermediate_table.Count() == 0) {
					gstate.ht->InitializeScan(gstate.ht_scan_state);
					gstate.key_source_phase = RecursiveCTEKeySourcePhase::DRAINING_FINAL_STATE;
				}
				break;
			}
			case RecursiveCTEKeySourcePhase::DRAINING_FINAL_STATE: {
				auto &payload_rows = gstate.source_payload_rows;
				auto &distinct_rows = gstate.source_distinct_rows;
				while (gstate.ht->Scan(gstate.ht_scan_state, distinct_rows, payload_rows)) {
					if (distinct_rows.size() == 0) {
						continue;
					}
					ScatterChunk(chunk, distinct_rows, distinct_idx);
					ScatterChunk(chunk, payload_rows, payload_idx);
					chunk.SetCardinalityUnsafe(distinct_rows.size());
					if (gstate.collect_runtime_metrics) {
						gstate.cumulative_final_state_rows += chunk.size();
					}
					return SourceResultType::HAVE_MORE_OUTPUT;
				}
				gstate.key_source_phase = RecursiveCTEKeySourcePhase::FINISHED;
				break;
			}
			case RecursiveCTEKeySourcePhase::FINISHED:
				return SourceResultType::FINISHED;
			default:
				throw InternalException("Unsupported recursive CTE key source phase");
			}
		}
	}
	while (chunk.size() == 0) {
		if (!gstate.finished_scan) {
			if (!using_key) {
				// scan any chunks we have collected so far
				gstate.CurrentOutputTable().Scan(gstate.scan_state, chunk);
			}
			if (chunk.size() == 0) {
				gstate.finished_scan = true;
			} else {
				break;
			}
		} else {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			auto &current_output = gstate.CurrentOutputTable();

			// After an iteration, we reset the recurring table
			// and fill it up with the new hash table rows for the next iteration.
			if (ref_recurring && current_output.Count() != 0) {
				// we need to populate the recurring table from the intermediate table
				// careful: we can not just use Combine here, because this destroys the intermediate table
				// instead we need to scan and append to create a copy
				// Note: as we are in the "normal" recursion case here, not the USING KEY case,
				// we can just scan the intermediate table directly, instead of going through the HT
				ColumnDataScanState scan_state;
				current_output.InitializeScan(scan_state);
				while (current_output.Scan(scan_state, gstate.source_result)) {
					recurring_table->Append(gstate.recurring_append_state, gstate.source_result);
				}
			}

			gstate.finished_scan = false;
			gstate.AdvanceIterationBuffers();
			gstate.ResetCurrentOutputTableForReuse();
			gstate.RebindRecursiveScans();

			// Pre-grow the dedup HT to avoid costly Resize + ReinsertTuples during the next Sink phase.
			// current_output.Count() is the count of rows output in the previous iteration — an upper bound
			// on the number of new unique rows the next iteration can add (since the recursion is converging).
			if (!union_all) {
				const idx_t expected_new = current_output.Count();
				if (expected_new > 0) {
					if (gstate.distinct_partitions.empty()) {
						const idx_t desired_capacity =
						    GroupedAggregateHashTable::GetCapacityForCount(gstate.ht->Count() + expected_new);
						if (desired_capacity > gstate.ht->Capacity()) {
							gstate.ht->Resize(desired_capacity);
						}
					} else {
						const auto expected_per_partition =
						    (expected_new + gstate.distinct_partitions.size() - 1) / gstate.distinct_partitions.size();
						for (auto &partition : gstate.distinct_partitions) {
							const auto desired_capacity = GroupedAggregateHashTable::GetCapacityForCount(
							    partition->ht.Count() + expected_per_partition);
							if (desired_capacity > partition->ht.Capacity()) {
								partition->ht.Resize(desired_capacity);
							}
						}
					}
				}
			}

			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.CurrentOutputTable().Count() == 0) {
				gstate.finished_scan = true;
				break;
			}
			// set up the scan again
			gstate.CurrentOutputTable().InitializeScan(gstate.scan_state);
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

vector<const_reference<PhysicalOperator>> PhysicalRecursiveCTE::GetSources() const {
	return {*this};
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename.GetIdentifierName();
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
