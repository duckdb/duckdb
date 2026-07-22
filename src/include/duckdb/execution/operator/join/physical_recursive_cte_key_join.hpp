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

//! Validated key and projection ordinals for a recursive state probe.
class RecursiveCTEKeyJoinLayout {
public:
	RecursiveCTEKeyJoinLayout(PhysicalRecursiveCTEStateScan &state_scan, PhysicalOperator &probe, bool state_on_left,
	                          vector<idx_t> state_key_indices, vector<idx_t> probe_key_indices,
	                          vector<idx_t> left_projection_map, vector<idx_t> right_projection_map);

	PhysicalRecursiveCTEStateScan &StateScan() const {
		return state_scan;
	}
	bool StateOnLeft() const {
		return state_on_left;
	}
	bool IsPartial() const;
	const vector<idx_t> &StateKeyIndices() const {
		return state_key_indices;
	}
	const vector<idx_t> &ProbeKeyIndices() const {
		return probe_key_indices;
	}
	const vector<idx_t> &LeftProjectionMap() const {
		return left_projection_map;
	}
	const vector<idx_t> &RightProjectionMap() const {
		return right_projection_map;
	}
	const vector<idx_t> &StateKeyMap() const {
		return state_key_map;
	}
	const vector<idx_t> &StatePayloadMap() const {
		return state_payload_map;
	}
	const vector<LogicalType> &KeyTypes() const {
		return key_types;
	}
	const vector<LogicalType> &ProbeKeyTypes() const {
		return probe_key_types;
	}
	const vector<LogicalType> &PayloadTypes() const {
		return payload_types;
	}

private:
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
};

//! Probes a frozen USING KEY recursive state using a complete or indexed partial equality key.
class PhysicalRecursiveCTEKeyJoin : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_KEY_JOIN;

	PhysicalRecursiveCTEKeyJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &probe,
	                            PhysicalRecursiveCTEStateScan &state_scan, bool state_on_left,
	                            vector<idx_t> state_key_indices, vector<idx_t> probe_key_indices,
	                            vector<idx_t> left_projection_map, vector<idx_t> right_projection_map,
	                            idx_t estimated_cardinality);

	PhysicalRecursiveCTEStateScan &StateScan() const {
		return layout.StateScan();
	}
	const RecursiveCTEKeyJoinLayout &Layout() const {
		return layout;
	}

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

private:
	RecursiveCTEKeyJoinLayout layout;
};

} // namespace duckdb
