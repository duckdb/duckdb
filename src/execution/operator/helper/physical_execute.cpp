#include "duckdb/execution/operator/helper/physical_execute.hpp"

namespace duckdb {

PhysicalExecute::PhysicalExecute(PhysicalOperator *plan)
    : PhysicalOperator(PhysicalOperatorType::EXECUTE, plan->types, -1), plan(plan) {
}

} // namespace duckdb
