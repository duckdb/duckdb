#include "duckdb/execution/operator/join/physical_join.hpp"

using namespace duckdb;
using namespace std;

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type)
    : PhysicalSink(type, op.types), join_type(join_type) {
}
