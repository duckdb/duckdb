//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_recursive_cte_key_join.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class LogicalComparisonJoin;
class PhysicalRecursiveCTEStateScan;

//! Probes a frozen USING KEY recursive state using a complete or indexed partial equality key.
class PhysicalRecursiveCTEKeyJoin : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_KEY_JOIN;

	PhysicalRecursiveCTEKeyJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &probe,
	                            PhysicalRecursiveCTEStateScan &state_scan, bool state_on_left,
	                            vector<idx_t> state_key_indices, vector<idx_t> probe_key_indices,
	                            vector<idx_t> left_projection_map, vector<idx_t> right_projection_map,
	                            idx_t estimated_cardinality);

	PhysicalRecursiveCTEStateScan &state_scan;
	bool state_on_left;
	vector<idx_t> state_key_indices;
	vector<idx_t> probe_key_indices;
	vector<idx_t> left_projection_map;
	vector<idx_t> right_projection_map;
	vector<idx_t> state_key_map;
	vector<idx_t> state_payload_map;
	vector<LogicalType> key_types;
	vector<LogicalType> probe_key_types;
	vector<LogicalType> payload_types;

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
};

} // namespace duckdb
