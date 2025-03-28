//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_use_bf.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"

namespace duckdb {
class PhysicalUseBF : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::USE_BF;

public:
	PhysicalUseBF(vector<LogicalType> types, const shared_ptr<FilterPlan> &filter_plan, shared_ptr<BloomFilter> bf,
	              PhysicalCreateBF *related_create_bfs, idx_t estimated_cardinality);

	shared_ptr<FilterPlan> filter_plan;
	PhysicalCreateBF *related_creator = nullptr;

	shared_ptr<BloomFilter> bf_to_use;

public:
	/* Operator interface */
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
};
} // namespace duckdb
