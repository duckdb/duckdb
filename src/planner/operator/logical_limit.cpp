#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

LogicalLimit::LogicalLimit(BoundLimitNode limit_val, BoundLimitNode offset_val)
    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT), limit_val(std::move(limit_val)),
      offset_val(std::move(offset_val)) {
}

vector<ColumnBinding> LogicalLimit::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

idx_t LogicalLimit::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = children[0]->EstimateCardinality(context);
	// A percentage limit applies to the complete input; the offset only caps the remaining rows.
	auto available_cardinality = child_cardinality;
	if (offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		auto offset = offset_val.GetConstantValue();
		available_cardinality = offset >= child_cardinality ? 0 : child_cardinality - offset;
	}
	auto cardinality = available_cardinality;
	switch (limit_val.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		cardinality = MinValue(cardinality, limit_val.GetConstantValue());
		break;
	case LimitNodeType::CONSTANT_PERCENTAGE: {
		// GetConstantPercentage() returns the raw percentage (e.g. 50 for 50%); divide by 100 and clamp to the
		// child cardinality so the idx_t conversion cannot overflow for a large child cardinality
		auto percentage = limit_val.GetConstantPercentage();
		auto percentage_cardinality = child_cardinality;
		if (percentage < 100) {
			percentage_cardinality = LossyNumericCast<idx_t>(double(child_cardinality) * percentage / 100.0);
		}
		cardinality = MinValue(cardinality, percentage_cardinality);
		break;
	}
	default:
		break;
	}
	return cardinality;
}

void LogicalLimit::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
