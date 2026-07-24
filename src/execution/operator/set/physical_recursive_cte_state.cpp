#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"

namespace duckdb {

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

void RecursiveCTESchedulerState::PrepareExecutorEntry(Pipeline &pipeline) {
	cached_executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>());
}

void RecursiveCTESchedulerState::PrepareExecutors(Pipeline &pipeline, idx_t max_threads) {
	// Recursive CTEs re-enter pipelines repeatedly, so retain executors locally and recycle them across states.
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

} // namespace duckdb
