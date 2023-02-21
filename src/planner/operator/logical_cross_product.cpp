#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

LogicalCrossProduct::LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_CROSS_PRODUCT, std::move(left), std::move(right)) {
}

unique_ptr<LogicalOperator> LogicalCrossProduct::Create(unique_ptr<LogicalOperator> left,
                                                        unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_unique<LogicalCrossProduct>(std::move(left), std::move(right));
}

void LogicalCrossProduct::Serialize(FieldWriter &writer) const {
}

unique_ptr<LogicalOperator> LogicalCrossProduct::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	// TODO(stephwang): review if unique_ptr<LogicalOperator> plan is needed
	auto result = unique_ptr<LogicalCrossProduct>(new LogicalCrossProduct());
	return std::move(result);
}

} // namespace duckdb
