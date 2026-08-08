#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

LogicalDependentJoin::LogicalDependentJoin(JoinType join_type)
    : LogicalJoin(join_type, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
}

} // namespace duckdb
