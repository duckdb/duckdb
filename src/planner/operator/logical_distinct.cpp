#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

LogicalDistinct::LogicalDistinct(DistinctType distinct_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_type(distinct_type) {
}
LogicalDistinct::LogicalDistinct(vector<unique_ptr<Expression>> targets, DistinctType distinct_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_type(distinct_type),
      distinct_targets(std::move(targets)) {
}

InsertionOrderPreservingMap<string> LogicalDistinct::ParamsToString() const {
	auto result = LogicalOperator::ParamsToString();
	if (!distinct_targets.empty()) {
		result["Distinct Targets"] =
		    StringUtil::Join(distinct_targets, distinct_targets.size(), "\n",
		                     [](const unique_ptr<Expression> &child) { return child->GetName(); });
	}
	SetParamsEstimatedCardinality(result);
	return result;
}

void LogicalDistinct::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
