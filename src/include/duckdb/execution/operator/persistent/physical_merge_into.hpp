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

class MergeIntoOperator {
public:
	// Merge action type
	MergeActionType action_type;
	//! Condition - or NULL if this should always be performed for the given action
	unique_ptr<Expression> condition;
	//! The operator to push data into for this action (if any)
	optional_ptr<PhysicalOperator> op;
	//! Expressions to execute (if any) prior to sinking
	vector<unique_ptr<Expression>> expressions;
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
	PhysicalMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types,
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

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
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

private:
	idx_t GetIndex(MergeActionCondition condition) const;
	void ComputeMatches(MergeIntoLocalState &local_state, DataChunk &chunk) const;
};

} // namespace duckdb
