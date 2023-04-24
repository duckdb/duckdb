//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_asof_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

//! LogicalAsOfJoin represents a temporal-style join with one less-than inequality.
//! This inequality matches the greatest value on the right that satisfies the condition.
class LogicalAsOfJoin : public LogicalComparisonJoin {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ASOF_JOIN;

public:
	explicit LogicalAsOfJoin(JoinType type);

	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
};

} // namespace duckdb
