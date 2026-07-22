#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/main/settings.hpp"

namespace duckdb {

RecursiveCTEPartialKeySpec::RecursiveCTEPartialKeySpec(vector<idx_t> indices_p, idx_t full_key_count)
    : indices(std::move(indices_p)) {
	if (indices.empty() || indices.size() >= full_key_count || !std::is_sorted(indices.begin(), indices.end()) ||
	    std::adjacent_find(indices.begin(), indices.end()) != indices.end() || indices.back() >= full_key_count) {
		throw InternalException("Invalid USING KEY partial-key index specification");
	}
}

RecursiveCTEPartialKeyIndex::RecursiveCTEPartialKeyIndex(Allocator &allocator,
                                                         const vector<LogicalType> &full_key_types,
                                                         vector<idx_t> key_indices_p)
    : key_indices(std::move(key_indices_p)), hashes(LogicalType::HASH) {
	vector<LogicalType> partial_key_types;
	for (auto key_idx : key_indices) {
		D_ASSERT(key_idx < full_key_types.size());
		partial_key_types.push_back(full_key_types[key_idx]);
	}
	partial_keys.Initialize(allocator, partial_key_types);
	selected_keys.Initialize(allocator, partial_key_types);
	Resize(1024);
}

void RecursiveCTEPartialKeyIndex::Resize(idx_t capacity) {
	D_ASSERT(capacity > 0 && (capacity & (capacity - 1)) == 0);
	heads.assign(capacity, DConstants::INVALID_INDEX);
	for (idx_t entry_idx = 0; entry_idx < entries.size(); entry_idx++) {
		auto &entry = entries[entry_idx];
		const auto bucket = entry.hash & (capacity - 1);
		entry.next = heads[bucket];
		heads[bucket] = entry_idx;
	}
}

void RecursiveCTEPartialKeyIndex::AddGroups(DataChunk &full_keys, const SelectionVector &new_groups,
                                            Vector &new_group_addresses, idx_t new_group_count) {
	if (new_group_count == 0) {
		return;
	}
	while (entries.size() + new_group_count > heads.size()) {
		Resize(heads.size() * 2);
	}
	partial_keys.Reset();
	for (idx_t partial_idx = 0; partial_idx < key_indices.size(); partial_idx++) {
		partial_keys.data[partial_idx].Reference(full_keys.data[key_indices[partial_idx]]);
	}
	partial_keys.CheckCardinality(full_keys.size());
	selected_keys.Reset();
	selected_keys.Slice(partial_keys, new_groups, new_group_count);
	selected_keys.Hash(hashes);

	const auto hash_values = hashes.Values<hash_t>();
	const auto addresses = FlatVector::GetData<data_ptr_t>(new_group_addresses);
	for (idx_t new_group_idx = 0; new_group_idx < new_group_count; new_group_idx++) {
		const auto hash = hash_values[new_group_idx].GetValue();
		const auto bucket = hash & (heads.size() - 1);
		entries.push_back({hash, addresses[new_group_idx], heads[bucket]});
		heads[bucket] = entries.size() - 1;
	}
}

idx_t RecursiveCTEPartialKeyIndex::GetHead(hash_t hash) const {
	return heads[hash & (heads.size() - 1)];
}

const RecursiveCTEPartialKeyIndex::Entry &RecursiveCTEPartialKeyIndex::GetEntry(idx_t entry_idx) const {
	return entries[entry_idx];
}

idx_t RecursiveCTEPartialKeyIndex::Count() const {
	return entries.size();
}

idx_t RecursiveCTEPartialKeyIndex::SizeInBytes() const {
	return heads.capacity() * sizeof(idx_t) + entries.capacity() * sizeof(Entry);
}

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
RecursiveCTEMetrics::RecursiveCTEMetrics(ClientContext &context, const PhysicalRecursiveCTE &op_p)
    : op(op_p), logger(context.logger),
      enabled(logger && logger->ShouldLog(PhysicalOperatorLogType::NAME, PhysicalOperatorLogType::LEVEL)) {
}

void RecursiveCTEMetrics::RecordTasks(idx_t count) {
	scheduled_tasks.fetch_add(count);
}

void RecursiveCTEMetrics::RecordEpoch(idx_t workers, idx_t elapsed_us_p, idx_t frontier_rows_p, idx_t frontier_chunks_p,
                                      idx_t frontier_storage_bytes_p) {
	epochs++;
	scheduled_workers += workers;
	elapsed_us += elapsed_us_p;
	frontier_rows += frontier_rows_p;
	frontier_chunks += frontier_chunks_p;
	frontier_storage_bytes += frontier_storage_bytes_p;
}

void RecursiveCTEMetrics::RecordSink(idx_t wait_ns, idx_t work_ns, idx_t rows) {
	sink_wait_ns.fetch_add(wait_ns);
	sink_work_ns.fetch_add(work_ns);
	sink_rows.fetch_add(rows);
	sink_calls.fetch_add(1);
}

void RecursiveCTEMetrics::RecordHashRows(idx_t rows) {
	hash_rows.fetch_add(rows);
}

void RecursiveCTEMetrics::RecordRecurringScanRows(idx_t rows) {
	recurring_scan_rows.fetch_add(rows);
}

void RecursiveCTEMetrics::RecordDirectProbeRows(idx_t rows) {
	direct_probe_rows.fetch_add(rows);
}

void RecursiveCTEMetrics::RecordDirectProbeMatches(idx_t rows) {
	direct_probe_matches.fetch_add(rows);
}

void RecursiveCTEMetrics::RecordPartialProbeChainVisit() {
	partial_probe_chain_visits.fetch_add(1);
}

void RecursiveCTEMetrics::RecordPartialIndexBuild(idx_t elapsed_us_p) {
	partial_index_build_us += elapsed_us_p;
}

void RecursiveCTEMetrics::RecordFinalStateRows(idx_t rows) {
	final_state_rows += rows;
}

void RecursiveCTEMetrics::RecordRetainedBuild() {
	retained_build_executions++;
}

void RecursiveCTEMetrics::RecordRetainedCTEMaterialization() {
	retained_cte_materializations++;
}

void RecursiveCTEMetrics::RecordRetainedCTEReuse() {
	retained_cte_reuses++;
}

void RecursiveCTEMetrics::LogDistinctPromotion(idx_t partitions, idx_t migrated_rows, idx_t elapsed_us_p) const {
	if (!enabled) {
		return;
	}
	DUCKDB_LOG(logger, PhysicalOperatorLogType, op, "PhysicalRecursiveCTE", "DistinctPromoted",
	           {{"partitions", to_string(partitions)},
	            {"migrated_rows", to_string(migrated_rows)},
	            {"elapsed_us", to_string(elapsed_us_p)}});
}

void RecursiveCTEMetrics::Log(const vector<unique_ptr<RecursiveCTEPartialKeyIndex>> &partial_key_indexes) const {
	if (!enabled) {
		return;
	}
	idx_t partial_index_rows = 0;
	idx_t partial_index_bytes = 0;
	for (auto &index : partial_key_indexes) {
		partial_index_rows += index->Count();
		partial_index_bytes += index->SizeInBytes();
	}
	DUCKDB_LOG(logger, PhysicalOperatorLogType, op, "PhysicalRecursiveCTE", "RuntimeMetrics",
	           {{"epochs", to_string(epochs)},
	            {"scheduled_workers", to_string(scheduled_workers)},
	            {"scheduled_tasks", to_string(scheduled_tasks.load())},
	            {"elapsed_us", to_string(elapsed_us)},
	            {"frontier_rows", to_string(frontier_rows)},
	            {"frontier_chunks", to_string(frontier_chunks)},
	            {"frontier_storage_bytes", to_string(frontier_storage_bytes)},
	            {"sink_lock_wait_ns", to_string(sink_wait_ns.load())},
	            {"sink_lock_work_ns", to_string(sink_work_ns.load())},
	            {"sink_lock_rows", to_string(sink_rows.load())},
	            {"sink_lock_calls", to_string(sink_calls.load())},
	            {"hash_rows", to_string(hash_rows.load())},
	            {"recurring_scan_rows", to_string(recurring_scan_rows.load())},
	            {"direct_probe_rows", to_string(direct_probe_rows.load())},
	            {"direct_probe_matches", to_string(direct_probe_matches.load())},
	            {"partial_probe_chain_visits", to_string(partial_probe_chain_visits.load())},
	            {"partial_index_build_us", to_string(partial_index_build_us)},
	            {"partial_index_rows", to_string(partial_index_rows)},
	            {"partial_index_bytes", to_string(partial_index_bytes)},
	            {"final_state_rows", to_string(final_state_rows)},
	            {"retained_build_executions", to_string(retained_build_executions)},
	            {"retained_cte_materializations", to_string(retained_cte_materializations)},
	            {"retained_cte_reuses", to_string(retained_cte_reuses)}});
}

RecursiveCTESchedulerState::RecursiveCTESchedulerState(shared_ptr<RecursiveExecutorPool> executor_pool_p,
                                                       bool allow_executor_reuse_p)
    : executor_pool(std::move(executor_pool_p)), allow_executor_reuse(allow_executor_reuse_p) {
}

RecursiveCTESchedulerState::~RecursiveCTESchedulerState() {
	ClearExecutors();
}

unique_ptr<RecursiveCTEPipelineSchedulePlan> &RecursiveCTESchedulerState::GetSchedulePlan(bool invariant) {
	return invariant ? invariant_schedule_plan : schedule_plan;
}

void RecursiveCTESchedulerState::InitializeInlinePlan(const RecursiveCTEPipelineSchedulePlan &plan) {
	remaining_schedule_dependencies.clear();
	remaining_schedule_dependencies.reserve(plan.stages.size());
	ready_schedule_stages.clear();
	ready_schedule_stages.reserve(plan.stages.size());
	for (idx_t stage_idx = 0; stage_idx < plan.stages.size(); stage_idx++) {
		auto dependency_count = plan.stages[stage_idx].dependency_count;
		remaining_schedule_dependencies.push_back(dependency_count);
		if (dependency_count == 0) {
			ready_schedule_stages.push_back(stage_idx);
		}
	}
}

idx_t RecursiveCTESchedulerState::ReadyStageCount() const {
	return ready_schedule_stages.size();
}

idx_t RecursiveCTESchedulerState::ReadyStage(idx_t index) const {
	return ready_schedule_stages[index];
}

void RecursiveCTESchedulerState::CompleteInlineStage(const RecursiveCTEPipelineSchedulePlan &plan, idx_t stage_idx) {
	for (auto dependent_stage : plan.stages[stage_idx].dependents) {
		auto &remaining = remaining_schedule_dependencies[dependent_stage];
		if (remaining == 0) {
			throw InternalException("Recursive inline schedule dependency underflow");
		}
		remaining--;
		if (remaining == 0) {
			ready_schedule_stages.push_back(dependent_stage);
		}
	}
}

void RecursiveCTESchedulerState::SetEpochThreadLimit(idx_t limit) {
	recursive_epoch_thread_limit = MaxValue<idx_t>(limit, 1);
}

idx_t RecursiveCTESchedulerState::EpochThreadLimit() const {
	return recursive_epoch_thread_limit;
}

RecursiveCTEState::RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
    : op(op), executor(context), new_group_addresses(LogicalType::POINTER), new_groups(STANDARD_VECTOR_SIZE),
      allow_executor_reuse(Settings::Get<EnableCachingOperatorsSetting>(context)), metrics(context, op),
      scheduler(op.shared_executor_pool, allow_executor_reuse),
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
		for (auto &spec : op.partial_key_index_specs) {
			partial_key_indexes.push_back(
			    make_uniq<RecursiveCTEPartialKeyIndex>(Allocator::Get(context), op.distinct_types, spec.Indices()));
		}
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
	metrics.Log(partial_key_indexes);
}

RecursiveCTEPartialKeyIndex &RecursiveCTEState::GetPartialKeyIndex(const vector<idx_t> &key_indices) {
	for (auto &index : partial_key_indexes) {
		if (index->key_indices == key_indices) {
			return *index;
		}
	}
	throw InternalException("USING KEY partial-key index is missing");
}

void RecursiveCTEState::RecordSinkMetrics(idx_t wait_ns, idx_t work_ns, idx_t rows) {
	metrics.RecordSink(wait_ns, work_ns, rows);
}

void RecursiveCTEState::InitializeIntermediateAppend() {
	intermediate_table.InitializeAppend(intermediate_append_state);
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
	InitializeIntermediateAppend();
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

void RecursiveCTESchedulerState::PrepareExecutorEntry(Pipeline &pipeline) {
	cached_executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>());
}

void RecursiveCTESchedulerState::PrepareExecutors(Pipeline &pipeline, idx_t max_threads) {
	// Ordinary pipelines build PipelineExecutors once and discard them after the query finishes.
	// Recursive CTEs re-enter the same pipelines many times, and correlated recursive invocations can
	// construct several RecursiveCTEState objects for the same physical plan. Keep a state-local cache
	// for cheap per-iteration reuse, and spill back into a shared pool so later states can recycle the
	// already-initialized executors instead of reconstructing them from scratch.
	auto entry = cached_executors.find(pipeline);
	if (entry == cached_executors.end()) {
		throw InternalException("Missing recursive pipeline executor cache entry");
	}
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

vector<unique_ptr<PipelineExecutor>> &RecursiveCTESchedulerState::GetExecutors(Pipeline &pipeline) {
	auto entry = cached_executors.find(pipeline);
	if (entry == cached_executors.end()) {
		throw InternalException("Missing recursive pipeline executor cache entry");
	}
	return entry->second;
}

void RecursiveCTESchedulerState::ClearExecutors() {
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
	    gstate.metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	idx_t new_group_count;
	{
		lock_guard<mutex> guard(gstate.intermediate_table_lock);
		const auto after_lock =
		    gstate.metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		if (gstate.metrics.Enabled()) {
			gstate.metrics.RecordHashRows(chunk.size());
		}
		new_group_count = gstate.ht->FindOrCreateGroups(chunk, lstate.dummy_addresses, lstate.new_groups);
		chunk.Slice(lstate.new_groups, new_group_count);
		if (gstate.metrics.Enabled()) {
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
		const auto collect_sink_metrics = gstate.metrics.Enabled() && record_sink_metrics;
		const auto before_lock =
		    collect_sink_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		idx_t new_group_count;
		{
			lock_guard<mutex> guard(partition.lock);
			const auto after_lock =
			    collect_sink_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			if (gstate.metrics.Enabled()) {
				gstate.metrics.RecordHashRows(partition_count);
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
	    metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
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
	if (metrics.Enabled()) {
		const auto promotion_end = std::chrono::steady_clock::now();
		const auto elapsed_us = NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::microseconds>(promotion_end - promotion_start).count());
		metrics.LogDistinctPromotion(partition_count, migrated_rows, elapsed_us);
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
		if (metrics.Enabled()) {
			metrics.RecordHashRows(update_rows.size());
		}
		distinct_rows.Reset();
		GatherChunk(distinct_rows, update_rows, op.distinct_idx);
		if (!executor.expressions.empty()) {
			payload_rows.Reset();
			executor.Execute(update_rows, payload_rows);
		}
		if (partial_key_indexes.empty()) {
			ht->AddChunk(distinct_rows, payload_rows, AggregateType::NON_DISTINCT);
			continue;
		}
		const auto build_start =
		    metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		const auto new_group_count = ht->AddChunkAndGetNewGroups(
		    distinct_rows, payload_rows, AggregateType::NON_DISTINCT, new_group_addresses, new_groups);
		for (auto &index : partial_key_indexes) {
			index->AddGroups(distinct_rows, new_groups, new_group_addresses, new_group_count);
		}
		if (metrics.Enabled()) {
			const auto build_end = std::chrono::steady_clock::now();
			metrics.RecordPartialIndexBuild(NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::microseconds>(build_end - build_start).count()));
		}
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
	RecursiveCTEStateScanLocalState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : found_groups(STANDARD_VECTOR_SIZE), arena(Allocator::Get(context)), row_state(arena) {
		distinct_rows.Initialize(Allocator::Get(context), op.distinct_types);
		payload_rows.Initialize(Allocator::Get(context), op.payload_types);
	}

	DataChunk distinct_rows;
	DataChunk payload_rows;
	AggregateHTLookupState lookup_state;
	SelectionVector found_groups;
	ArenaAllocator arena;
	RowOperationsState row_state;
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
	while (true) {
		{
			lock_guard<mutex> guard(gstate.lock);
			if (!gstate.initialized) {
				recursive_state.ht->InitializeScan(gstate.scan_state);
				gstate.initialized = true;
			}
			if (!recursive_state.ht->ScanGroups(gstate.scan_state, lstate.distinct_rows)) {
				return SourceResultType::FINISHED;
			}
		}
		if (lstate.distinct_rows.size() == 0) {
			continue;
		}
		const auto group_count = lstate.distinct_rows.size();
		const auto found_count =
		    recursive_state.ht->LookupGroups(lstate.distinct_rows, lstate.lookup_state, lstate.found_groups);
		if (found_count != group_count) {
			throw InternalException("USING KEY state scan could not find %d of %d frozen groups",
			                        group_count - found_count, group_count);
		}
		lstate.payload_rows.Reset();
		lstate.payload_rows.SetChildCardinality(group_count);
		if (lstate.payload_rows.ColumnCount() > 0) {
			lock_guard<mutex> finalize_guard(recursive_state.ht_finalize_lock);
			auto layout = recursive_state.ht->GetLayoutPtr();
			RowOperations::FinalizeStates(lstate.row_state, *layout, lstate.lookup_state.addresses, lstate.payload_rows,
			                              0);
		}
		ScatterChunk(chunk, lstate.distinct_rows, distinct_idx);
		ScatterChunk(chunk, lstate.payload_rows, payload_idx);
		chunk.CheckCardinality(lstate.distinct_rows.size());
		if (recursive_state.metrics.Enabled()) {
			recursive_state.metrics.RecordRecurringScanRows(chunk.size());
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
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
		if (!gstate.use_local_union_all_output) {
			const auto before_lock =
			    gstate.metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			{
				lock_guard<mutex> guard(gstate.intermediate_table_lock);
				const auto after_lock = gstate.metrics.Enabled() ? std::chrono::steady_clock::now()
				                                                 : std::chrono::steady_clock::time_point();
				gstate.CurrentOutputTable().Append(gstate.CurrentOutputAppendState(), chunk);
				if (gstate.metrics.Enabled()) {
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
	    gstate.metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	{
		lock_guard<mutex> guard(gstate.intermediate_table_lock);
		const auto after_lock =
		    gstate.metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		// Collect updates without mutating the hash state read by recurring.T in this epoch.
		gstate.intermediate_table.Append(gstate.intermediate_append_state, chunk);
		if (gstate.metrics.Enabled()) {
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
		if (union_all && !gstate.use_local_union_all_output) {
			return SinkCombineResultType::FINISHED;
		}
		auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
		D_ASSERT(lstate.output);
		const auto before_lock =
		    gstate.metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		{
			lock_guard<mutex> guard(gstate.intermediate_table_lock);
			const auto after_lock =
			    gstate.metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			const auto row_count = lstate.output->Count();
			gstate.CurrentOutputTable().Combine(*lstate.output);
			if (gstate.metrics.Enabled()) {
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
					chunk.CheckCardinality(distinct_rows.size());
					if (gstate.metrics.Enabled()) {
						gstate.metrics.RecordFinalStateRows(chunk.size());
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
