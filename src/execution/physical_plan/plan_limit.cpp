#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

bool UseBatchLimit(PhysicalOperator &child_node, BoundLimitNode &limit_val, BoundLimitNode &offset_val) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	return true;
#else
	// we only want to use the batch limit when we are executing a complex query (e.g. involving a filter or join)
	// if we are doing a limit over a table scan we are otherwise scanning a lot of rows just to throw them away
	reference<PhysicalOperator> current_ref(child_node);
	bool finished = false;
	while (!finished) {
		auto &current_op = current_ref.get();
		switch (current_op.type) {
		case PhysicalOperatorType::TABLE_SCAN:
			return false;
		case PhysicalOperatorType::PROJECTION:
			current_ref = current_op.children[0];
			break;
		default:
			finished = true;
			break;
		}
	}
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

static bool TryPushLimitIntoChild(LogicalLimit &op) {
	if (op.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return false;
	}
	if (op.offset_val.Type() != LimitNodeType::CONSTANT_VALUE && op.offset_val.Type() != LimitNodeType::UNSET) {
		return false;
	}

	idx_t limit_value = op.limit_val.GetConstantValue();
	idx_t offset_value = 0;
	if (op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		offset_value = op.offset_val.GetConstantValue();
	}
	idx_t total = limit_value + offset_value;

	// Unpartitioned hash tables become expensive to Combine at large sizes
	static constexpr idx_t LIMITED_DISTINCT_THRESHOLD = 100000;
	if (total > LIMITED_DISTINCT_THRESHOLD) {
		return false;
	}

	auto &child = *op.children[0];
	if (child.type == LogicalOperatorType::LOGICAL_DISTINCT) {
		child.Cast<LogicalDistinct>().limit = total;
		return true;
	}
	if (child.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate = child.Cast<LogicalAggregate>();
		if (aggregate.expressions.empty() && !aggregate.groups.empty()) {
			aggregate.limit = total;
			return true;
		}
	}
	return false;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalLimit &op) {
	D_ASSERT(op.children.size() == 1);

	// Try to push limit hint into DISTINCT or GROUP BY for early termination
	// The hint tells the child to stop collecting groups once it has enough.
	// We still create a physical LIMIT node on top to enforce the actual limit/offset.
	TryPushLimitIntoChild(op);

	auto &plan = CreatePlan(*op.children[0]);

	switch (op.limit_val.Type()) {
	case LimitNodeType::EXPRESSION_PERCENTAGE:
	case LimitNodeType::CONSTANT_PERCENTAGE: {
		auto &limit = Make<PhysicalLimitPercent>(op.types, std::move(op.limit_val), std::move(op.offset_val),
		                                         op.estimated_cardinality);
		limit.children.push_back(plan);
		return limit;
	}
	default: {
		if (!PreserveInsertionOrder(plan)) {
			// use parallel streaming limit if insertion order is not important
			auto &limit = Make<PhysicalStreamingLimit>(op.types, std::move(op.limit_val), std::move(op.offset_val),
			                                           op.estimated_cardinality, true);
			limit.children.push_back(plan);
			return limit;
		}

		// maintaining insertion order is important
		if (UseBatchIndex(plan) && UseBatchLimit(plan, op.limit_val, op.offset_val)) {
			// source supports batch index: use parallel batch limit
			auto &limit = Make<PhysicalLimit>(op.types, std::move(op.limit_val), std::move(op.offset_val),
			                                  op.estimated_cardinality);
			limit.children.push_back(plan);
			return limit;
		}

		// source does not support batch index: use a non-parallel streaming limit
		auto &limit = Make<PhysicalStreamingLimit>(op.types, std::move(op.limit_val), std::move(op.offset_val),
		                                           op.estimated_cardinality, false);
		limit.children.push_back(plan);
		return limit;
	}
	}
}

} // namespace duckdb
