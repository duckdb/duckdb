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
	void ClearExecutors();
	unique_ptr<RecursiveCTEPipelineSchedulePlan> &GetSchedulePlan(bool invariant);
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
	unique_ptr<RecursiveCTEPipelineSchedulePlan> schedule_plan;
	unique_ptr<RecursiveCTEPipelineSchedulePlan> invariant_schedule_plan;
	vector<idx_t> remaining_schedule_dependencies;
	vector<idx_t> ready_schedule_stages;
	idx_t recursive_epoch_thread_limit = 1;
};

class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op);
	~RecursiveCTEState() override;

	void InitializeIntermediateAppend();
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
	RecursiveCTEMetrics metrics;
	RecursiveCTESchedulerState scheduler;

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

	bool use_local_union_all_output = true;
	//! Whether invariant recursive meta-pipelines have already been materialized for this state
	bool invariant_meta_pipelines_materialized = false;
};

} // namespace duckdb
