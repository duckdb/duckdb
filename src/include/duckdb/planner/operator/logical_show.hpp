//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_show.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalShow : public LogicalOperator {
	LogicalShow() : LogicalOperator(LogicalOperatorType::LOGICAL_SHOW) {};

public:
	explicit LogicalShow(unique_ptr<LogicalOperator> plan) : LogicalOperator(LogicalOperatorType::LOGICAL_SHOW) {
		children.push_back(std::move(plan));
	}

	vector<LogicalType> types_select;
	vector<string> aliases;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(0, 0), ColumnBinding(0, 1), ColumnBinding(0, 2),
		        ColumnBinding(0, 3), ColumnBinding(0, 4), ColumnBinding(0, 5)};
	}
};
} // namespace duckdb
