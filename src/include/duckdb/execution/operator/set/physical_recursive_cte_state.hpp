#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

struct RecursiveExecutorPool {
	mutex lock;
	PhysicalRecursiveCTE::executor_cache_t executors;
};

enum class RecursiveCTEInlineStageType : uint8_t { EXECUTE, PREPARE_FINISH, FINISH };

struct RecursiveCTEInlineStage {
	RecursiveCTEInlineStage(RecursiveCTEInlineStageType type_p, Pipeline &pipeline_p)
	    : type(type_p), pipeline(pipeline_p), dependency_count(0) {
	}

	RecursiveCTEInlineStageType type;
	reference<Pipeline> pipeline;
	vector<idx_t> dependents;
	idx_t dependency_count;
};

struct RecursiveCTEInlinePlan {
	vector<RecursiveCTEInlineStage> stages;
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
	vector<unique_ptr<PipelineExecutor>> &GetCachedExecutors(Pipeline &pipeline, idx_t max_threads);
	void ClearCachedExecutors();

	unique_ptr<GroupedAggregateHashTable> ht;
	const PhysicalRecursiveCTE &op;
	ExpressionExecutor executor;
	DataChunk payload_rows;
	const bool allow_executor_reuse;
	shared_ptr<RecursiveExecutorPool> executor_pool;

	mutex intermediate_table_lock;
	ColumnDataCollection intermediate_table;
	ColumnDataAppendState intermediate_append_state;
	ColumnDataAppendState working_append_state;
	ColumnDataAppendState recurring_append_state;
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished_scan = false;
	bool output_is_working = false;
	SelectionVector new_groups;
	//! Cached dummy address vector for ProbeHT (avoids per-chunk VectorBuffer allocation)
	Vector dummy_addresses;
	//! Cached chunk for distinct key extraction in the using_key Sink path
	DataChunk distinct_rows;
	//! Cached chunks for source-side hash table scans and recurring table copy paths
	DataChunk source_result;
	DataChunk source_payload_rows;
	DataChunk source_distinct_rows;
	AggregateHTScanState ht_scan_state;

	//! Cached PipelineExecutors per pipeline for reuse across recursive iterations
	//! When both locks are needed, always acquire cached_executor_lock before executor_pool->lock.
	mutex cached_executor_lock;
	PhysicalRecursiveCTE::executor_cache_t cached_executors;
	//! Cached dependency graph for the single-thread inline recursive fast path
	unique_ptr<RecursiveCTEInlinePlan> inline_plan;
	//! Cached dependency graph after invariant meta-pipelines have been materialized once
	unique_ptr<RecursiveCTEInlinePlan> invariant_inline_plan;
	//! Whether invariant recursive meta-pipelines have already been materialized for this state
	bool invariant_meta_pipelines_materialized = false;
};

} // namespace duckdb
