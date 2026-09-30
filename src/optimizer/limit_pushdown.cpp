#include "duckdb/optimizer/limit_pushdown.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/optimizer/constraint_propagation/constraint_propagator.hpp"
#include "duckdb/optimizer/constraint_propagation/queries.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

bool LimitPushdown::CanPushThroughProjection(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &limit = op.Cast<LogicalLimit>();

		if (limit.offset_val.Type() == LimitNodeType::EXPRESSION_PERCENTAGE ||
		    limit.offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			// Offset cannot be an expression
			return false;
		}

		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.limit_val.GetConstantValue() < MAX_LIMIT) {
			// Push down only when limit value is smaller than 8192.
			// when physical_limit is introduced, it will end a parallel pipeline
			// restrict the limit value to be small so that remaining operations run fast without parallelization.
			if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.offset_val.GetConstantValue() > 0) {
				// If we push the limit below the projection, the offset rows are discarded before the
				// projection runs, so a volatile expression produces different values for the rows we keep.
				auto &projection = op.children[0]->Cast<LogicalProjection>();
				for (auto &expr : projection.expressions) {
					if (expr->IsVolatile()) {
						return false;
					}
				}
			}
			return true;
		}
	}
	return false;
}

bool LimitPushdown::HasLimit(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT) {
		return true;
	}
	for (auto &child : op.children) {
		if (HasLimit(*child)) {
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> LimitPushdown::Optimize(unique_ptr<LogicalOperator> op) {
	if (!HasLimit(*op)) {
		return op;
	}

	for (int iteration = 0; iteration < 10; iteration++) {
		ConstraintPropagator propagator;
		propagator.Analyze(*op);

		bool join_pushed = false;
		bool changed = false;
		op = OptimizeInternal(std::move(op), propagator, join_pushed, changed);
		if (!changed) {
			break;
		}
	}
	return op;
}

unique_ptr<LogicalOperator> LimitPushdown::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                            ConstraintPropagator &propagator, bool &join_pushed,
                                                            bool &changed) {
	if (CanPushThroughProjection(*op)) {
		auto projection = std::move(op->children[0]);
		op->children[0] = std::move(projection->children[0]);
		projection->SetEstimatedCardinality(op->estimated_cardinality);
		projection->children[0] = std::move(op);
		swap(projection, op);
		changed = true;
	}

	if (!join_pushed && op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto child_type = op->children[0]->type;
		if (child_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    child_type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			op = TryPushIntoJoin(std::move(op), propagator, join_pushed);
			if (join_pushed) {
				changed = true;
			}
		}
	}

	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child), propagator, join_pushed, changed);
	}
	return op;
}

unique_ptr<LogicalOperator> LimitPushdown::TryPushIntoJoin(unique_ptr<LogicalOperator> op,
                                                           ConstraintPropagator &propagator, bool &pushed) {
	auto &limit = op->Cast<LogicalLimit>();

	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return op;
	}

	switch (limit.offset_val.Type()) {
	case LimitNodeType::UNSET:
	case LimitNodeType::CONSTANT_VALUE:
		break;
	default:
		return op;
	}

	if (limit.limit_val.GetConstantValue() >= MAX_LIMIT) {
		return op;
	}

	auto &join = op->children[0]->Cast<LogicalComparisonJoin>();

	if (join.filter_pushdown) {
		return op;
	}

	auto qualifies = [&](idx_t side) {
		return MultiplicityOf(propagator, join, side) >= SideMultiplicity::EXACTLY_ONE;
	};

	idx_t push_side = 0;
	switch (join.join_type) {
	case JoinType::LEFT:
	case JoinType::SEMI:
	case JoinType::MARK:
	case JoinType::SINGLE:
		break;
	case JoinType::RIGHT:
		push_side = 1;
		break;
	case JoinType::INNER:
		push_side = 0;
		if (!qualifies(0) && qualifies(1)) {
			push_side = 1;
		}
		break;
	default:
		return op;
	}

	if (!qualifies(push_side)) {
		return op;
	}

	auto join_op = std::move(op->children[0]);
	join_op->SetEstimatedCardinality(op->estimated_cardinality);
	op->children[0] = std::move(join_op->children[push_side]);
	join_op->children[push_side] = std::move(op);
	pushed = true;
	return join_op;
}

} // namespace duckdb
