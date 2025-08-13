#include "duckdb/optimizer/common_subplan_optimizer.hpp"

namespace duckdb {

CommonSubplanOptimizer::CommonSubplanOptimizer(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> CommonSubplanOptimizer::Optimize(unique_ptr<LogicalOperator> op) {

	return op;
}

} // namespace duckdb
