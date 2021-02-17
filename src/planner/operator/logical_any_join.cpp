#include "duckdb/planner/operator/logical_any_join.hpp"

namespace duckdb {

LogicalAnyJoin::LogicalAnyJoin(JoinType type) : LogicalJoin(type, LogicalOperatorType::LOGICAL_ANY_JOIN) {
}

string LogicalAnyJoin::ParamsToString() const {
	return condition->ToString();
}

} // namespace duckdb
