#include "duckdb/planner/operator/logical_asof_join.hpp"

namespace duckdb {

LogicalAsOfJoin::LogicalAsOfJoin(JoinType type) : LogicalComparisonJoin(type, LogicalOperatorType::LOGICAL_ASOF_JOIN) {
}

unique_ptr<LogicalOperator> LogicalAsOfJoin::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto result = make_uniq<LogicalAsOfJoin>(JoinType::INVALID);
	LogicalComparisonJoin::Deserialize(*result, state, reader);
	return std::move(result);
}

} // namespace duckdb
