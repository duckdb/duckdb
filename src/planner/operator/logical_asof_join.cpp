#include "duckdb/planner/operator/logical_asof_join.hpp"

namespace duckdb {

LogicalAsOfJoin::LogicalAsOfJoin(JoinType type) : LogicalComparisonJoin(type, LogicalOperatorType::LOGICAL_ASOF_JOIN) {
}

} // namespace duckdb
