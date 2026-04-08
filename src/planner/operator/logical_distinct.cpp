#include <string>
#include <utility>

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
enum class DistinctType : uint8_t;

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
