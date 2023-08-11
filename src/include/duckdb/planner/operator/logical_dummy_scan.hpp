//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDummyScan represents a dummy scan returning a single row
class LogicalDummyScan : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_DUMMY_SCAN;

public:
	explicit LogicalDummyScan(idx_t table_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DUMMY_SCAN), table_index(table_index) {
		logical_type = LogicalOperatorType::LOGICAL_DUMMY_SCAN;
		m_derived_property_relation = new CDrvdPropRelational();
		m_group_expression = nullptr;
		m_derived_property_plan = nullptr;
		m_required_plan_property = nullptr;
	}

	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(table_index, 0)};
	}

	idx_t EstimateCardinality(ClientContext &context) override {
		return 1;
	}
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	vector<idx_t> GetTableIndex() const override;

protected:
	void ResolveTypes() override {
		if (types.empty()) {
			types.emplace_back(LogicalType::INTEGER);
		}
	}
};
} // namespace duckdb
