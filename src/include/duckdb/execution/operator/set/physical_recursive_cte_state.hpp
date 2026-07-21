#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

class Logger;
struct RecursiveCTEDistinctPartition;

struct RecursiveExecutorPool {
	mutex lock;
	PhysicalRecursiveCTE::executor_cache_t executors;
};

enum class RecursiveCTEInlineStageType : uint8_t { EXECUTE, PREPARE_FINISH, FINISH };

enum class RecursiveCTEKeySourcePhase : uint8_t { RECURSING, DRAINING_FINAL_STATE, FINISHED };

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
	RecursiveCTEScheduleStage(RecursiveCTEInlineStageType type_p, Pipeline &pipeline_p)
	    : type(type_p), pipeline(pipeline_p), dependency_count(0) {
	}

	RecursiveCTEInlineStageType type;
	reference<Pipeline> pipeline;
	vector<idx_t> dependents;
	idx_t dependency_count;
};

struct RecursiveCTEPipelineSchedulePlan {
	vector<RecursiveCTEScheduleStage> stages;
	vector<reference<Pipeline>> initialize_on_schedule_pipelines;
};

class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op);
	~RecursiveCTEState() override;

	void InitializeIntermediateAppend();
	void ResetRecurringTable();
	ColumnDataCollection &CurrentOutputTable();
	ColumnDataCollection &CurrentInputTable();
	const ColumnDataCollection &CurrentInputTable() const;
	ColumnDataAppendState &CurrentOutputAppendState();
	void AdvanceIterationBuffers();
	void ResetCurrentOutputTableForReuse();
	void RebindRecursiveScans();
	void CommitUsingKeyUpdates();
	void PromoteDistinctState(ClientContext &context, idx_t partition_count);
	void RecordSinkMetrics(idx_t wait_ns, idx_t work_ns, idx_t rows);
	void PrepareCachedExecutorEntry(Pipeline &pipeline);
	void PrepareCachedExecutors(Pipeline &pipeline, idx_t max_threads);
	vector<unique_ptr<PipelineExecutor>> &GetCachedExecutors(Pipeline &pipeline);
	void ClearCachedExecutors();
	RecursiveCTEPartialKeyIndex &GetPartialKeyIndex(const vector<idx_t> &key_indices);

	unique_ptr<GroupedAggregateHashTable> ht;
	vector<unique_ptr<RecursiveCTEPartialKeyIndex>> partial_key_indexes;
	vector<unique_ptr<RecursiveCTEDistinctPartition>> distinct_partitions;
	const PhysicalRecursiveCTE &op;
	ExpressionExecutor executor;
	DataChunk payload_rows;
	Vector new_group_addresses;
	SelectionVector new_groups;
	const bool allow_executor_reuse;
	shared_ptr<Logger> runtime_logger;
	const bool collect_runtime_metrics;
	shared_ptr<RecursiveExecutorPool> executor_pool;

	mutex intermediate_table_lock;
	mutex ht_finalize_lock;
	ColumnDataCollection intermediate_table;
	ColumnDataAppendState intermediate_append_state;
	ColumnDataAppendState working_append_state;
	ColumnDataAppendState recurring_append_state;
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished_scan = false;
	RecursiveCTEKeySourcePhase key_source_phase = RecursiveCTEKeySourcePhase::RECURSING;
	bool output_is_working = false;
	//! Cached chunk for distinct key extraction in the using_key Sink path
	DataChunk distinct_rows;
	//! Cached chunks for source-side hash table scans and recurring table copy paths
	DataChunk source_result;
	DataChunk update_rows;
	DataChunk source_payload_rows;
	DataChunk source_distinct_rows;
	AggregateHTScanState ht_scan_state;

	//! Cached PipelineExecutors per pipeline for reuse across recursive iterations
	PhysicalRecursiveCTE::executor_cache_t cached_executors;
	//! Immutable recursive pipeline topology, shared by inline and event execution
	unique_ptr<RecursiveCTEPipelineSchedulePlan> schedule_plan;
	//! Cached dependency graph after invariant meta-pipelines have been materialized once
	unique_ptr<RecursiveCTEPipelineSchedulePlan> invariant_schedule_plan;
	//! Epoch-reset runtime for scheduler-free execution
	vector<idx_t> remaining_schedule_dependencies;
	vector<idx_t> ready_schedule_stages;
	//! Physical scheduling state (not exposed through profiling or serialization)
	idx_t recursive_epoch_thread_limit = 1;
	bool use_local_union_all_output = true;
	idx_t cumulative_epoch_count = 0;
	idx_t cumulative_worker_count = 0;
	atomic<idx_t> cumulative_task_count {0};
	idx_t cumulative_elapsed_us = 0;
	idx_t cumulative_frontier_rows = 0;
	idx_t cumulative_frontier_chunks = 0;
	idx_t cumulative_frontier_storage_bytes = 0;
	atomic<idx_t> cumulative_sink_wait_ns {0};
	atomic<idx_t> cumulative_sink_work_ns {0};
	atomic<idx_t> cumulative_sink_rows {0};
	atomic<idx_t> cumulative_sink_calls {0};
	atomic<idx_t> cumulative_hash_rows {0};
	atomic<idx_t> cumulative_recurring_scan_rows {0};
	atomic<idx_t> cumulative_direct_probe_rows {0};
	atomic<idx_t> cumulative_direct_probe_matches {0};
	atomic<idx_t> cumulative_partial_probe_chain_visits {0};
	idx_t cumulative_partial_index_build_us = 0;
	idx_t cumulative_final_state_rows = 0;
	idx_t retained_build_executions = 0;
	//! Whether invariant recursive meta-pipelines have already been materialized for this state
	bool invariant_meta_pipelines_materialized = false;
};

} // namespace duckdb
