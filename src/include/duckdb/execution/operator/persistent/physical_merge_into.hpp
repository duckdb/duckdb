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
class PhysicalPlanGenerator;
class PipelineBroadcastExchange;

//! The source of a MERGE INTO action pipeline - the merge into pushes the rows that belong to this action into the
//! queue of the action, which this operator scans. The pipeline blocks while no data is available.
class PhysicalMergeActionSource : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::MERGE_ACTION_SOURCE;

public:
	PhysicalMergeActionSource(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                          MergeActionCondition condition, MergeActionType action_type, bool parallel);
	~PhysicalMergeActionSource() override;

	MergeActionCondition condition;
	MergeActionType action_type;
	//! Whether or not the rows of this action can be consumed by multiple threads
	bool parallel;
	//! The exchange that the merge into pushes the rows of this action into - set up when building the pipelines.
	//! This is shared because the merge into keeps the exchange alive: the physical plan can destroy this source
	//! before the merge into, whose sink state cancels the exchange when it is torn down.
	shared_ptr<PipelineBroadcastExchange> exchange;
	//! The consumer of the exchange that this source scans - every action has an exchange of its own, so the rows
	//! of an action are routed to it by pushing them into its exchange
	idx_t consumer_idx = 0;
	//! The plan that produces the rows that the merge into pushes into this source. This is not a child - the rows
	//! are pushed in by the merge into - but the source stands in for it when the operators of the action are
	//! planned, so plan walks that look for the origin of the rows can continue here.
	optional_ptr<PhysicalOperator> merge_input;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;
	void SourceFinished(ClientContext &context, GlobalSourceState &gstate) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return parallel;
	}
	OrderPreservationType SourceOrder() const override {
		//! rows are handed out to the consumers in whichever order they pick them up
		return OrderPreservationType::NO_ORDER;
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
	optional_ptr<PhysicalMergeActionSource> source;
	//! Projection that emits the RETURNING data of this action - set up by the PhysicalMergeInto
	optional_ptr<PhysicalOperator> returning_projection;
};

//! Plans the source that feeds the operators of the given merge action, together with a projection for the
//! expressions of the action (if any). The result must be used as the child plan when planning the operators of the
//! action, so that the merge into can push the rows of the action into them.
PhysicalOperator &PlanMergeActionSource(PhysicalPlanGenerator &planner, PhysicalOperator &merge_input,
                                        MergeActionCondition condition, MergeIntoOperator &action);

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
	                  optional_idx source_marker, bool parallel, bool return_chunk, bool serialize_actions = false);

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
	//! Whether the actions must run one after the other instead of concurrently - required when multiple actions
	//! append to the same table, which their operators cannot do concurrently
	bool serialize_actions;

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
