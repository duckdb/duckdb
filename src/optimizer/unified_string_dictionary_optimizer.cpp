#include "duckdb/optimizer/unified_string_dictionary_optimizer.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/operator/logical_unified_string_dictionary_insertion.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

unique_ptr<LogicalOperator>
UnifiedStringDictionaryOptimizer::CheckIfUnifiedStringDictionaryRequired(unique_ptr<LogicalOperator> op) {
	auto rewrite_result = Rewrite(std::move(op));
	if (rewrite_result.target_operator_found) {
		optimizer->context.UnifiedStringDictionary = make_uniq<UnifiedStringsDictionary>(1ull);
	}
	return std::move(rewrite_result.op);
}

bool UnifiedStringDictionaryOptimizer::CheckIfTargetOperatorAndInsert(optional_ptr<LogicalOperator> op) {
	bool isTargetOperator = false;
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr_op = op->Cast<LogicalAggregate>();
		// TryAddCompressedGroup in hash aggregate is far more effective for single columns
		if (aggr_op.groups.size() == 1) {
			break;
		}
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
		// TryAddCompressedGroup in hash aggregate is far more effective for single columns
		if (distinct_op.distinct_targets.size() == 1) {
			break;
		}
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
		// TODO: filter operator is also another possible TargetOperator
	default:
		break;
	}

	if (isTargetOperator) {
		for (idx_t i = 0; i < op->children.size(); ++i) {
			vector<bool> usd_insert_vec;
			for (auto &type : op->children[i]->types) {
				if (type == LogicalType::VARCHAR) {
					usd_insert_vec.push_back(true);
				} else {
					usd_insert_vec.push_back(false);
				}
			}

			bool insert_flat_vecs = false;
			if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				insert_flat_vecs = EnableFlatVecInsertion(op->children[i], op->children[1 - i]);
			}

			auto new_operator =
			    make_uniq<LogicalUnifiedStringDictionaryInsertion>(std::move(usd_insert_vec), insert_flat_vecs);

			if (op->children[i]->has_estimated_cardinality) {
				new_operator->SetEstimatedCardinality(op->children[i]->estimated_cardinality);
			}
			new_operator->children.push_back(std::move(op->children[i]));
			op->children[i] = std::move(new_operator);

			op->ResolveOperatorTypes();
		}
		return true;
	}
	return false;
}

bool UnifiedStringDictionaryOptimizer::EnableFlatVecInsertion(optional_ptr<LogicalOperator> op,
                                                              optional_ptr<LogicalOperator> neighbor_op) {
	if (op->has_estimated_cardinality && neighbor_op->has_estimated_cardinality &&
	    op->estimated_cardinality < JOIN_CARDINALITY_THRESHOLD && op->estimated_cardinality > 0) {
		return (neighbor_op->estimated_cardinality / op->estimated_cardinality) > JOIN_CARDINALITY_RATIO_THRESHOLD;
	}
	return false;
}

UnifiedStringDictionaryOptimizerContext UnifiedStringDictionaryOptimizer::Rewrite(unique_ptr<LogicalOperator> op) {
	op->ResolveOperatorTypes();

	auto target_operator_result = false;
	// Depth-first-search post-order
	for (idx_t i = 0; i < op->children.size(); ++i) {
		auto rewrite_result = Rewrite(std::move(op->children[i]));
		target_operator_result |= rewrite_result.target_operator_found;
		op->children[i] = std::move(rewrite_result.op);
	}
	if (!target_operator_result) {
		target_operator_result |= CheckIfTargetOperatorAndInsert(op);
	}
	return {std::move(op), target_operator_result};
}

} // namespace duckdb
