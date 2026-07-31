//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

class RecursiveCTEState;
class RecursiveCTEMetrics;
struct RecursiveExecutorPool;
struct RecursiveCTEPipelineSchedulePlan;
class PhysicalColumnDataScan;
class Pipeline;
class PipelineExecutor;

class RecursiveCTEPartialKeySpec {
public:
	RecursiveCTEPartialKeySpec(vector<idx_t> indices, idx_t full_key_count);

	const vector<idx_t> &Indices() const {
		return indices;
	}
	bool operator==(const RecursiveCTEPartialKeySpec &other) const {
		return indices == other.indices;
	}

private:
	vector<idx_t> indices;
};

struct RecursiveCTEReferenceInfo {
	idx_t frontier_scans = 0;
	idx_t recurring_scans = 0;
	idx_t exact_key_probes = 0;
	idx_t partial_key_probes = 0;
	idx_t direct_probe_work_units = 0;

	bool HasReferences() const {
		return frontier_scans > 0 || recurring_scans > 0 || exact_key_probes > 0 || partial_key_probes > 0;
	}
};

class PhysicalRecursiveCTE : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_CTE;
	using executor_cache_t = reference_map_t<Pipeline, vector<unique_ptr<PipelineExecutor>>>;
	friend class RecursiveCTEState;
	friend class RecursiveCTEMetrics;

public:
	PhysicalRecursiveCTE(PhysicalPlan &physical_plan, Identifier ctename, TableIndex table_index,
	                     vector<LogicalType> types, bool union_all, PhysicalOperator &top, PhysicalOperator &bottom,
	                     idx_t estimated_cardinality);
	~PhysicalRecursiveCTE() override;

	Identifier ctename;
	TableIndex table_index;
	// Flag if recurring table is referenced, if not we do not copy ht into ColumnDataCollection
	bool ref_recurring;
	bool union_all;
	shared_ptr<ColumnDataCollection> working_table;
	shared_ptr<MetaPipeline> recursive_meta_pipeline;

	//===--------------------------------------------------------------------===//
	// Additionally required for using-key recursive CTE to normal CTE.
	//===--------------------------------------------------------------------===//
	bool using_key = false;
	// Contains the result of the key variant
	shared_ptr<ColumnDataCollection> recurring_table;
	// Contains the types of the payload and key columns.
	vector<LogicalType> payload_types, distinct_types, internal_types;
	// Contains the payload and key indices
	vector<idx_t> payload_idx, distinct_idx;
	// Contains the aggregates for the payload
	vector<unique_ptr<Expression>> payload_aggregates;
	//! Physical-only partial-key indexes required by direct recursive state probes.
	vector<RecursiveCTEPartialKeySpec> partial_key_index_specs;
	//! Physical recursive inputs inside the recursive member
	RecursiveCTEReferenceInfo recursive_references;
	//! Recursive table scans rebound to the current iteration input buffer
	vector<reference<PhysicalColumnDataScan>> recursive_scans;
	//! Recursive meta-pipelines that are independent of the active recursive scan graph and can be materialized once
	reference_set_t<const MetaPipeline> invariant_meta_pipelines;
	//! Physical operators whose results cannot be retained across recursive iterations
	reference_set_t<const PhysicalOperator> non_repeatable_operators;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	void PrepareFinalize(ClientContext &context, GlobalSinkState &sink_state) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	vector<const_reference<PhysicalOperator>> GetSources() const override;

private:
	void ExecuteRecursivePipelines(ExecutionContext &context) const;
	idx_t NextMetricsInvocation() const;

private:
	mutable shared_ptr<RecursiveExecutorPool> shared_executor_pool;
	//! Immutable recursive projections of the generic pipeline schedule.
	unique_ptr<RecursiveCTEPipelineSchedulePlan> recursive_schedule_plan;
	unique_ptr<RecursiveCTEPipelineSchedulePlan> invariant_recursive_schedule_plan;
	mutable atomic<idx_t> metrics_invocations {0};
};

//! Scans the frozen USING KEY state during a recursive epoch.
class PhysicalRecursiveCTEStateScan : public PhysicalColumnDataScan {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN;

	PhysicalRecursiveCTEStateScan(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                              TableIndex cte_index);

	optional_ptr<PhysicalRecursiveCTE> recursive_cte;
	vector<idx_t> distinct_idx;
	vector<idx_t> payload_idx;
	vector<RecursiveCTEPartialKeySpec> partial_key_index_specs;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context,
	                                                   const OperatorPartitionInfo &partition_info) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	SourceResultType GetDataFromState(DataChunk &chunk, OperatorSourceInput &input,
	                                  RecursiveCTEState &recursive_state) const;
};

} // namespace duckdb
