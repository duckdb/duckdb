#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/parallel/pipeline_schedule.hpp"

namespace duckdb {

class Logger;
class RecursiveCTELocalState;
struct RecursiveCTEDistinctPartition;

struct RecursiveExecutorPool {
	mutex lock;
	PhysicalRecursiveCTE::executor_cache_t executors;
};

enum class RecursiveCTESourcePhase : uint8_t {
	INITIAL,
	SCANNING_UNION,
	RECURSING_KEY,
	DRAINING_FINAL_KEY_STATE,
	FINISHED
};

//! Epoch-stable secondary index over a proper subset of USING KEY columns.
class RecursiveCTEPartialKeyIndex {
public:
	struct Entry {
		hash_t hash;
		data_ptr_t address;
		idx_t next;
	};

	RecursiveCTEPartialKeyIndex(Allocator &allocator, const vector<LogicalType> &full_key_types,
	                            vector<idx_t> key_indices);

	void AddGroups(DataChunk &full_keys, const SelectionVector &new_groups, Vector &new_group_addresses,
	               idx_t new_group_count);
	idx_t GetHead(hash_t hash) const;
	const Entry &GetEntry(idx_t entry_idx) const;
	idx_t Count() const;
	idx_t SizeInBytes() const;

	vector<idx_t> key_indices;

private:
	void Resize(idx_t capacity);

	DataChunk partial_keys;
	DataChunk selected_keys;
	Vector hashes;
	vector<idx_t> heads;
	vector<Entry> entries;
};

struct RecursiveCTEScheduleStage {
	RecursiveCTEScheduleStage(PipelineScheduleStageType type_p, Pipeline &pipeline_p, bool has_source_tasks_p)
	    : type(type_p), pipeline(pipeline_p), has_source_tasks(has_source_tasks_p), dependency_count(0) {
	}

	PipelineScheduleStageType type;
	reference<Pipeline> pipeline;
	bool has_source_tasks;
	vector<idx_t> dependents;
	idx_t dependency_count;
};

struct RecursiveCTEPipelineSchedulePlan {
	vector<RecursiveCTEScheduleStage> stages;
	vector<reference<Pipeline>> initialize_on_schedule_pipelines;
	idx_t execute_pipeline_count = 0;
	idx_t cte_scan_pipeline_count = 0;
	bool has_source_tasks = false;
	bool source_tasks_write_recursive_output = false;
};

class RecursiveCTEMetrics {
public:
	RecursiveCTEMetrics(ClientContext &context, const PhysicalRecursiveCTE &op);

	bool Enabled() const {
		return enabled;
	}
	void RecordTasks(idx_t count);
	void RecordEpoch(idx_t workers, idx_t elapsed_us, idx_t frontier_rows, idx_t frontier_chunks,
	                 idx_t frontier_storage_bytes);
	void RecordSink(idx_t wait_ns, idx_t work_ns, idx_t rows);
	void RecordHashRows(idx_t rows);
	void RecordRecurringScanRows(idx_t rows);
	void RecordDirectProbeRows(idx_t rows);
	void RecordDirectProbeMatches(idx_t rows);
	void RecordPartialProbeChainVisit();
	void RecordPartialIndexBuild(idx_t elapsed_us);
	void RecordFinalStateRows(idx_t rows);
	void RecordRetainedBuild();
	void RecordRetainedCTEMaterialization();
	void RecordRetainedCTEReuse();
	void LogDistinctPromotion(idx_t partitions, idx_t migrated_rows, idx_t elapsed_us) const;
	void Log(const vector<unique_ptr<RecursiveCTEPartialKeyIndex>> &partial_key_indexes) const;

private:
	const PhysicalRecursiveCTE &op;
	shared_ptr<Logger> logger;
	bool enabled;
	idx_t epochs = 0;
	idx_t scheduled_workers = 0;
	atomic<idx_t> scheduled_tasks {0};
	idx_t elapsed_us = 0;
	idx_t frontier_rows = 0;
	idx_t frontier_chunks = 0;
	idx_t frontier_storage_bytes = 0;
	atomic<idx_t> sink_wait_ns {0};
	atomic<idx_t> sink_work_ns {0};
	atomic<idx_t> sink_rows {0};
	atomic<idx_t> sink_calls {0};
	atomic<idx_t> hash_rows {0};
	atomic<idx_t> recurring_scan_rows {0};
	atomic<idx_t> direct_probe_rows {0};
	atomic<idx_t> direct_probe_matches {0};
	atomic<idx_t> partial_probe_chain_visits {0};
	idx_t partial_index_build_us = 0;
	idx_t final_state_rows = 0;
	idx_t retained_build_executions = 0;
	idx_t retained_cte_materializations = 0;
	idx_t retained_cte_reuses = 0;
};

class RecursiveCTESchedulerState {
public:
	RecursiveCTESchedulerState(shared_ptr<RecursiveExecutorPool> executor_pool, bool allow_executor_reuse);
	~RecursiveCTESchedulerState();

	void PrepareExecutorEntry(Pipeline &pipeline);
	void PrepareExecutors(Pipeline &pipeline, idx_t max_threads);
	vector<unique_ptr<PipelineExecutor>> &GetExecutors(Pipeline &pipeline);
	bool HasExecutorEntries() const;
	void ClearExecutors();
	void InitializeInlinePlan(const RecursiveCTEPipelineSchedulePlan &plan);
	idx_t ReadyStageCount() const;
	idx_t ReadyStage(idx_t index) const;
	void CompleteInlineStage(const RecursiveCTEPipelineSchedulePlan &plan, idx_t stage_idx);
	void SetEpochThreadLimit(idx_t limit);
	idx_t EpochThreadLimit() const;

private:
	shared_ptr<RecursiveExecutorPool> executor_pool;
	bool allow_executor_reuse;
	PhysicalRecursiveCTE::executor_cache_t cached_executors;
	vector<idx_t> remaining_schedule_dependencies;
	vector<idx_t> ready_schedule_stages;
	idx_t recursive_epoch_thread_limit = 1;
};

class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op);
	~RecursiveCTEState() override;

	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk);
	const ColumnDataCollection &CurrentInputTable() const;
	void InitializeSharedOutputAppend();
	void CommitUsingKeyUpdates();
	void PromoteDistinctState(ClientContext &context, idx_t partition_count);
	void RecordSinkMetrics(idx_t wait_ns, idx_t work_ns, idx_t rows);
	const RecursiveCTEPartialKeyIndex &GetPartialKeyIndex(const vector<idx_t> &key_indices) const;
	void AppendOutput(DataChunk &chunk);
	void CombineOutput(ColumnDataCollection &output);
	void SinkSerialDistinct(DataChunk &chunk, RecursiveCTELocalState &local_state);
	void SinkDistinct(DataChunk &chunk, RecursiveCTELocalState &local_state, bool emit_rows = true,
	                  bool record_sink_metrics = true);
	void FinalizePayload(RowOperationsState &row_state, Vector &addresses, DataChunk &payload, idx_t payload_idx);

	const PhysicalRecursiveCTE &GetOperator() const {
		return op;
	}
	GroupedAggregateHashTable &GetHashTable() {
		D_ASSERT(ht);
		return *ht;
	}
	const GroupedAggregateHashTable &GetHashTable() const {
		D_ASSERT(ht);
		return *ht;
	}
	RecursiveCTEMetrics &GetMetrics() {
		return metrics;
	}
	const RecursiveCTEMetrics &GetMetrics() const {
		return metrics;
	}
	RecursiveCTESchedulerState &GetScheduler() {
		return scheduler;
	}
	const RecursiveCTESchedulerState &GetScheduler() const {
		return scheduler;
	}
	bool AllowsExecutorReuse() const {
		return allow_executor_reuse;
	}
	bool UsesLocalUnionAllOutput() const {
		return use_local_union_all_output;
	}
	void SetUseLocalUnionAllOutput(bool value) {
		use_local_union_all_output = value;
	}
	bool HasDistinctPartitions() const {
		return !distinct_partitions.empty();
	}
	bool HasMaterializedInvariantPipelines() const {
		return invariant_meta_pipelines_materialized;
	}
	void MarkInvariantPipelinesMaterialized() {
		invariant_meta_pipelines_materialized = true;
	}

private:
	unique_ptr<GroupedAggregateHashTable> ht;
	vector<unique_ptr<RecursiveCTEPartialKeyIndex>> partial_key_indexes;
	vector<unique_ptr<RecursiveCTEDistinctPartition>> distinct_partitions;
	const PhysicalRecursiveCTE &op;
	ExpressionExecutor executor;
	DataChunk payload_rows;
	Vector new_group_addresses;
	SelectionVector new_groups;
	const bool allow_executor_reuse;
	RecursiveCTEMetrics metrics;
	RecursiveCTESchedulerState scheduler;

	mutex intermediate_table_lock;
	mutex ht_finalize_lock;
	ColumnDataCollection intermediate_table;
	ColumnDataAppendState intermediate_append_state;
	ColumnDataAppendState working_append_state;
	ColumnDataAppendState recurring_append_state;
	ColumnDataScanState scan_state;
	RecursiveCTESourcePhase source_phase = RecursiveCTESourcePhase::INITIAL;
	bool output_is_working = false;
	//! Cached chunk for distinct key extraction in the using_key Sink path
	DataChunk distinct_rows;
	//! Cached chunks for source-side hash table scans and recurring table copy paths
	DataChunk source_result;
	DataChunk update_rows;
	DataChunk source_payload_rows;
	DataChunk source_distinct_rows;
	AggregateHTScanState ht_scan_state;

	bool use_local_union_all_output = true;
	//! Whether invariant recursive meta-pipelines have already been materialized for this state
	bool invariant_meta_pipelines_materialized = false;

	SourceResultType GetUsingKeyData(ExecutionContext &context, DataChunk &chunk);
	SourceResultType GetUnionData(ExecutionContext &context, DataChunk &chunk);
	void InitializeIntermediateAppend();
	ColumnDataCollection &CurrentOutputTable();
	ColumnDataCollection &CurrentInputTable();
	ColumnDataAppendState &CurrentOutputAppendState();
	void AdvanceIterationBuffers();
	void ResetCurrentOutputTableForReuse();
	void RebindRecursiveScans();
};

} // namespace duckdb
