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
// TAGGED SUB-TYPES (e.g. LOGICAL_WITHIN_JOIN) share this class; TYPE is INVALID so
	// Cast<LogicalAnyJoin>() never rejects the tagged operator (same pattern as
	// LogicalComparisonJoin, which hosts ASOF/DELIM sub-types).
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	explicit LogicalAnyJoin(JoinType type);
	//! Convert a stable, non-volatile ANY join into a comparison join when possible.
	static void TrySpecialize(unique_ptr<LogicalOperator> &op);

	//! The JoinCondition on which this join is performed
	unique_ptr<Expression> condition;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
