//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_delim_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalHashAggregate;

//! PhysicalDelimJoin represents a join where either the LHS or RHS will be duplicate eliminated and pushed into a
//! PhysicalColumnDataScan in the other side. Implementations are PhysicalLeftDelimJoin and PhysicalRightDelimJoin
class PhysicalDelimJoin : public PhysicalOperator {
public:
	PhysicalDelimJoin(PhysicalOperatorType type, vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
	                  vector<const_reference<PhysicalOperator>> delim_scans, idx_t estimated_cardinality,
	                  optional_idx delim_idx);

	unique_ptr<PhysicalOperator> join;
	unique_ptr<PhysicalHashAggregate> distinct;
	vector<const_reference<PhysicalOperator>> delim_scans;

	optional_idx delim_idx;

public:
	vector<const_reference<PhysicalOperator>> GetChildren() const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	OrderPreservationType SourceOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}
	bool SinkOrderDependent() const override {
		return false;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
