//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_merge_into.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/enums/merge_action_type.hpp"

namespace duckdb {
class MergeIntoLocalState;
class Pipeline;

//! The source of a MERGE INTO action pipeline - the merge into pushes the rows that belong to this action directly
//! into the pipeline, this operator is never scanned
class PhysicalMergeActionSource : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::MERGE_ACTION_SOURCE;

public:
	PhysicalMergeActionSource(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                          MergeActionCondition condition, MergeActionType action_type);

	MergeActionCondition condition;
	MergeActionType action_type;

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

class MergeIntoOperator {
public:
	// Merge action type
	MergeActionType action_type;
	//! Condition - or NULL if this should always be performed for the given action
	unique_ptr<Expression> condition;
	//! The operator to push data into for this action (if any)
	optional_ptr<PhysicalOperator> op;
	//! Expressions to execute (if any) prior to sinking - these are turned into a projection by the merge into for
	//! actions that have an operator, and are only executed directly for the MERGE_ERROR action
	vector<unique_ptr<Expression>> expressions;
	//! The source that feeds `op` - set up by the PhysicalMergeInto
	optional_ptr<PhysicalOperator> source;
	//! Projection that emits the RETURNING data of this action - set up by the PhysicalMergeInto
	optional_ptr<PhysicalOperator> returning_projection;
};

struct MergeActionRange {
	MergeActionCondition condition;
	idx_t start = 0;
	idx_t end = 0;
};

class PhysicalMergeInto : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::MERGE_INTO;

public:
	PhysicalMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperator &child,
	                  map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions, idx_t row_id_index,
	                  optional_idx source_marker, bool parallel, bool return_chunk);

	//! List of all actions
	vector<unique_ptr<MergeIntoOperator>> actions;
	//! Sequence of match actions
	vector<MergeActionCondition> match_actions;
	//! List of all actions that apply to a given action condition
	vector<MergeActionRange> action_ranges;
	idx_t row_id_index;
	optional_idx source_marker;
	bool parallel;
	bool return_chunk;
	//! The pipelines of the actions that we push data into - set up when building the pipelines
	vector<reference<Pipeline>> action_pipelines;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		//! with RETURNING the data is emitted by the action operators instead
		return !return_chunk || action_pipeline_count == 0;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return parallel;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;

private:
	//! Set up the operators that the given action pushes data into
	void PlanAction(PhysicalPlan &physical_plan, MergeActionCondition condition, MergeIntoOperator &action);
	idx_t GetIndex(MergeActionCondition condition) const;
	void ComputeMatches(MergeIntoLocalState &local_state, DataChunk &chunk) const;

private:
	//! The number of actions that have an operator to push data into
	idx_t action_pipeline_count = 0;
};

} // namespace duckdb
