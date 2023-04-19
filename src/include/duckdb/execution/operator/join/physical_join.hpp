//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

//! PhysicalJoin represents the base class of the join operators
class PhysicalJoin : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;

public:
	PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type, idx_t estimated_cardinality);

	JoinType join_type;

public:
	bool EmptyResultIfRHSIsEmpty() const;

	static bool HasNullValues(DataChunk &chunk);
	static void ConstructSemiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]);
	static void ConstructAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]);
	static void ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &left, DataChunk &result, bool found_match[],
	                                    bool has_null);

public:
	static void BuildJoinPipelines(Pipeline &current, MetaPipeline &confluent_pipelines, PhysicalOperator &op);
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;

	OrderPreservationType SourceOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}
	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}
	bool SinkOrderDependent() const override {
		return false;
	}
};

} // namespace duckdb
