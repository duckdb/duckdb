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
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_SHOW;

public:
	explicit LogicalShow(unique_ptr<LogicalOperator> plan) : LogicalOperator(LogicalOperatorType::LOGICAL_SHOW) {
		children.push_back(std::move(plan));
	}

	vector<LogicalType> types_select;
	vector<string> aliases;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(0, types.size());
	}
};
} // namespace duckdb
