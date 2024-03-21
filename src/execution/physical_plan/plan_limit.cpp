#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

bool UseBatchLimit(BoundLimitNode &limit_val, BoundLimitNode &offset_val) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	return true;
#else
	// we only use batch limit when we are computing a small amount of values
	// as the batch limit materializes this many rows PER thread
	static constexpr const idx_t BATCH_LIMIT_THRESHOLD = 10000;

	if (limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return false;
	}
	if (offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
		return false;
	}
	idx_t total_offset = limit_val.GetConstantValue();
	if (offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		total_offset += offset_val.GetConstantValue();
	}
	return total_offset <= BATCH_LIMIT_THRESHOLD;
#endif
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimit &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	if (plan->type == PhysicalOperatorType::ORDER_BY && op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE &&
	    op.offset_val.Type() != LimitNodeType::EXPRESSION_VALUE) {
		auto &order_by = plan->Cast<PhysicalOrder>();
		// Can not use TopN operator if PhysicalOrder uses projections
		if (order_by.projections.empty()) {
			idx_t offset_val = 0;
			if (op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				offset_val = op.offset_val.GetConstantValue();
			}
			auto top_n = make_uniq<PhysicalTopN>(op.types, std::move(order_by.orders), op.limit_val.GetConstantValue(),
			                                     offset_val, op.estimated_cardinality);
			top_n->children.push_back(std::move(order_by.children[0]));
			return std::move(top_n);
		}
	}

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
			if (UseBatchIndex(*plan) && UseBatchLimit(op.limit_val, op.offset_val)) {
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
