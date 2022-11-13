//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_explain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"

namespace duckdb {

class LogicalExplain : public LogicalOperator {
	LogicalExplain(ExplainType explain_type)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPLAIN), explain_type(explain_type) {};

public:
	LogicalExplain(unique_ptr<LogicalOperator> plan, ExplainType explain_type)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPLAIN), explain_type(explain_type) {
		children.push_back(move(plan));
	}

	ExplainType explain_type;
	string physical_plan;
	string logical_plan_unopt;
	string logical_plan_opt;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	idx_t EstimateCardinality(ClientContext &context) override {
		return 3;
	}

protected:
	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(0, 0), ColumnBinding(0, 1)};
	}
};
} // namespace duckdb
