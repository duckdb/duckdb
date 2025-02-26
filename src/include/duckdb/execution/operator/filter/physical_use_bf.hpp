#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"

namespace duckdb {
class PhysicalUseBF : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::USE_BF;

public:
	PhysicalUseBF(vector<LogicalType> types, vector<shared_ptr<BlockedBloomFilter>> bf, idx_t estimated_cardinality);

	vector<shared_ptr<BlockedBloomFilter>> bf_to_use;
	vector<PhysicalCreateBF *> related_create_bf;

	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	/* Operator interface */
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
};
} // namespace duckdb