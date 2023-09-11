//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_positional_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/operator/logical_unconditional_join.hpp"

namespace duckdb {

//! LogicalPositionalJoin represents a row-wise join between two relations
class LogicalPositionalJoin : public LogicalUnconditionalJoin {
	LogicalPositionalJoin() : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_POSITIONAL_JOIN) {};

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_POSITIONAL_JOIN;

public:
	LogicalPositionalJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

public:
	static unique_ptr<LogicalOperator> Create(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
