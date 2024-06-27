#include "duckdb/planner/operator/logical_any_join.hpp"

namespace duckdb {

LogicalAnyJoin::LogicalAnyJoin(JoinType type) : LogicalJoin(type, LogicalOperatorType::LOGICAL_ANY_JOIN) {
}

case_insensitive_map_t<string> LogicalAnyJoin::ParamsToString() const {
	case_insensitive_map_t<string> result;
	result["Condition"] = condition->ToString();
	return result;
}

} // namespace duckdb
