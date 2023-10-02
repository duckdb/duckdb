#include "duckdb/planner/operator/logical_positional_join.hpp"

namespace duckdb {

LogicalPositionalJoin::LogicalPositionalJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_POSITIONAL_JOIN, std::move(left), std::move(right)) {
}

unique_ptr<LogicalOperator> LogicalPositionalJoin::Create(unique_ptr<LogicalOperator> left,
                                                          unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_uniq<LogicalPositionalJoin>(std::move(left), std::move(right));
}

} // namespace duckdb
