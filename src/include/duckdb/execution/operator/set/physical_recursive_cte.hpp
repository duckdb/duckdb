//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

class RecursiveCTEState;

class PhysicalRecursiveCTE : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_CTE;

public:
	PhysicalRecursiveCTE(PhysicalPlan &physical_plan, string ctename, idx_t table_index, vector<LogicalType> types,
	                     bool union_all, PhysicalOperator &top, PhysicalOperator &bottom, idx_t estimated_cardinality);
	~PhysicalRecursiveCTE() override;

	string ctename;
	idx_t table_index;
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
	vector<LogicalType> payload_types, distinct_types;
	// Contains the payload and key indices
	vector<idx_t> payload_idx, distinct_idx;
	// Contains the aggregates for the payload
	vector<unique_ptr<BoundAggregateExpression>> payload_aggregates;

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
	//! Probe Hash Table and eliminate duplicate rows
	idx_t ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const;

	void ExecuteRecursivePipelines(ExecutionContext &context) const;
};

} // namespace duckdb
