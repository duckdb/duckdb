//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_pos_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalPositionalJoin represents a row-wise join between two relations
class LogicalPositionalJoin : public LogicalOperator {
	LogicalPositionalJoin() : LogicalOperator(LogicalOperatorType::LOGICAL_POSITIONAL_JOIN) {};

public:
	LogicalPositionalJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

public:
	static unique_ptr<LogicalOperator> Create(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
