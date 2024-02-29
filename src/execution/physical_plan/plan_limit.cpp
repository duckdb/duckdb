#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimit &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	unique_ptr<PhysicalOperator> limit;
	switch (op.limit_val.Type()) {
	case LimitNodeType::EXPRESSION_PERCENTAGE:
	case LimitNodeType::CONSTANT_PERCENTAGE:
		limit = make_uniq<PhysicalLimitPercent>(op.types, std::move(op.limit_val), std::move(op.offset_val),
		                                        op.estimated_cardinality);
		break;
	default:
		if (!PreserveInsertionOrder(*plan)) {
			// use parallel streaming limit if insertion order is not important
			limit = make_uniq<PhysicalStreamingLimit>(op.types, std::move(op.limit_val), std::move(op.offset_val),
			                                          op.estimated_cardinality, true);
		} else {
			// maintaining insertion order is important
			if (UseBatchIndex(*plan)) {
				// source supports batch index: use parallel batch limit
				limit = make_uniq<PhysicalLimit>(op.types, std::move(op.limit_val), std::move(op.offset_val),
				                                 op.estimated_cardinality);
			} else {
				// source does not support batch index: use a non-parallel streaming limit
				limit = make_uniq<PhysicalStreamingLimit>(op.types, std::move(op.limit_val), std::move(op.offset_val),
				                                          op.estimated_cardinality, false);
			}
		}
		break;
	}

	limit->children.push_back(std::move(plan));
	return limit;
}

} // namespace duckdb
