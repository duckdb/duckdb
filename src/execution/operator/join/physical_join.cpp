#include "duckdb/execution/operator/join/physical_join.hpp"

using namespace std;

namespace duckdb {

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type)
    : PhysicalSink(type, op.types), join_type(join_type) {
}

}
