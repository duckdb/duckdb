//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_create_bf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {
class CreateBFGlobalSinkState;

class PhysicalCreateBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_BF;

public:
	PhysicalCreateBF(vector<LogicalType> types, const vector<shared_ptr<FilterPlan>> &filter_plans,
	                 vector<shared_ptr<DynamicTableFilterSet>> dynamic_filter_sets,
	                 vector<vector<ColumnBinding>> &dynamic_filter_cols, idx_t estimated_cardinality,
	                 bool is_probing_side);

	// We use a mutable boolean variable to mark if this operator successfully materializes and creates its BFs.
	// This variable is helpful for dynamic pipeline scheduling. It allows us to end the pipeline that has
	// PhysicalCreateBFs in advance.
	// TODO: we may need to remove the mutable.
	bool is_probing_side;
	mutable atomic<bool> is_successful;
	shared_ptr<Pipeline> this_pipeline;

	vector<shared_ptr<FilterPlan>> filter_plans;
	vector<shared_ptr<BloomFilter>> bf_to_create;
	vector<vector<ColumnBinding>> min_max_applied_cols;
	vector<shared_ptr<DynamicTableFilterSet>> min_max_to_create;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	// Add related createBF dependency
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	void BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline);

public:
	// If memory is not enough, give up creating BFs
	bool GiveUpBFCreation(const DataChunk &chunk, OperatorSinkInput &input) const;
};
} // namespace duckdb
