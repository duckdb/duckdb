#include "duckdb/execution/operator/join/physical_join.hpp"

namespace duckdb {

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type,
                           idx_t estimated_cardinality)
    : PhysicalOperator(type, op.types, estimated_cardinality), join_type(join_type) {
}

} // namespace duckdb
