//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_any_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! LogicalAnyJoin represents a join with an arbitrary expression as JoinCondition
class LogicalAnyJoin : public LogicalJoin {
public:
	explicit LogicalAnyJoin(JoinType type);

	//! The JoinCondition on which this join is performed
	unique_ptr<Expression> condition;

public:
	string ParamsToString() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
};

} // namespace duckdb
