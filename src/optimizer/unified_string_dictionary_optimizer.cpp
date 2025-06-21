#include "duckdb/optimizer/unified_string_dictionary_optimizer.h"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/operator/logical_unified_string_dictionary_insertion.h"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

unique_ptr<LogicalOperator>
UnifiedStringDictionaryOptimizer::CheckIfUnifiedStringDictionaryRequired(unique_ptr<LogicalOperator> op) {
	auto rewrite_result = Rewrite(std::move(op));
	if (rewrite_result.target_operator_found) {
		optimizer->context.UnifiedStringDictionary.reset();
		optimizer->context.UnifiedStringDictionary = make_uniq<UnifiedStringsDictionary>(1ull);
	}
	return std::move(rewrite_result.op);
}

void UnifiedStringDictionaryOptimizer::CheckIfTargetOperatorAndInsert(optional_ptr<LogicalOperator> op) {
	bool isTargetOperator = false;
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr_op = op->Cast<LogicalAggregate>();
		for (auto &expr : aggr_op.groups) {
			if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
				if (bound_colref.return_type == LogicalType::VARCHAR) {
					isTargetOperator = true;
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct_op = op->Cast<LogicalDistinct>();
		for (auto &expr : distinct_op.distinct_targets) {
			if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
				if (bound_colref.return_type == LogicalType::VARCHAR) {
					isTargetOperator = true;
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join_op = op->Cast<LogicalComparisonJoin>();
		// if the join condition contains strings
		for (auto &condition : join_op.conditions) {
			if (condition.left->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref = condition.left->Cast<BoundColumnRefExpression>();
				if (bound_colref.return_type == LogicalType::VARCHAR) {
					isTargetOperator = true;
				}
			}
			if (condition.right->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref = condition.right->Cast<BoundColumnRefExpression>();
				if (bound_colref.return_type == LogicalType::VARCHAR) {
					isTargetOperator = true;
				}
			}
			for (auto &type : join_op.types) {
				if (type == LogicalType::VARCHAR) {
					isTargetOperator = true;
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &sort_op = op->Cast<LogicalOrder>();
		for (auto &node : sort_op.orders) {
			if (node.expression->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref = node.expression->Cast<BoundColumnRefExpression>();
				if (bound_colref.return_type == LogicalType::VARCHAR) {
					isTargetOperator = true;
				}
			}
		}
		break;
	}
	default:
		break;
	}

	if(isTargetOperator){
		for (idx_t i = 0; i < op->children.size(); ++i) {
			vector<bool> usd_insert_vec;
			for (auto &type : op->children[i]->types) {
				if (type == LogicalType::VARCHAR) {
					usd_insert_vec.push_back(true);
				} else {
					usd_insert_vec.push_back(false);
				}
			}
			auto new_operator = make_uniq<LogicalUnifiedStringDictionaryInsertion>(std::move(usd_insert_vec));
			new_operator->children.push_back(std::move(op->children[i]));
			op->children[i] = std::move(new_operator);

			op->ResolveOperatorTypes();
		}
	}
}

UnifiedStringDictionaryOptimizerContext UnifiedStringDictionaryOptimizer::Rewrite(unique_ptr<LogicalOperator> op) {
	op->ResolveOperatorTypes();

	auto children_target_operator_result = false;
	// Depth-first-search post-order
	for (idx_t i = 0; i < op->children.size(); ++i) {
		auto rewrite_result = Rewrite(std::move(op->children[i]));
		children_target_operator_result |= rewrite_result.target_operator_found;
		op->children[i] = std::move(rewrite_result.op);
	}
	if(!children_target_operator_result){
		CheckIfTargetOperatorAndInsert(op);
	}
	return {std::move(op), children_target_operator_result};
}

} // namespace duckdb
