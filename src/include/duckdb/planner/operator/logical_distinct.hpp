//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_distinct.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

//! LogicalDistinct filters duplicate entries from its child operator
class LogicalDistinct : public LogicalOperator {
public:
	LogicalDistinct() : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT) {
	}
	explicit LogicalDistinct(vector<unique_ptr<Expression>> targets)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_targets(std::move(targets)) {
	}
	//! The set of distinct targets (optional).
	vector<unique_ptr<Expression>> distinct_targets;
	//! The order by modifier (optional, only for distinct on)
	unique_ptr<BoundOrderModifier> order_by;

public:
	string ParamsToString() const override;

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
