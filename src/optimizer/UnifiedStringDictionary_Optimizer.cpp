#include "duckdb/optimizer/UnifiedStringDictionary_Optimizer.h"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/operator/logical_ussr_insertion.h"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> USSR_optimizer::CheckUnifiedDictionary(unique_ptr<LogicalOperator> op) {
	op = Rewrite(std::move(op));
	for (auto &ds : chosen_data_sources) {
		Insert_USSR_Operator(ds);
	}
	return op;
}

void USSR_optimizer::Insert_USSR_Operator(optional_ptr<LogicalOperator> op) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		vector<bool> ussr_insert_vec;
		//		D_ASSERT(op->children[i]->type == LogicalOperatorType::LOGICAL_GET);
		for (auto &type : op->children[i]->types) {
			if (type == LogicalType::VARCHAR) {
				ussr_insert_vec.push_back(true);
			} else {
				ussr_insert_vec.push_back(false);
			}
		}

		auto new_operator = make_uniq<LogicalUSSRInsertion>(std::move(ussr_insert_vec));
		new_operator->children.push_back(std::move(op->children[i]));
		op->children[i] = std::move(new_operator);

		op->ResolveOperatorTypes();
	}
}

void USSR_optimizer::choose_operator() {
	for (auto &ds : candidate_data_sources) {
		chosen_data_sources.push_back(ds);
	}
	candidate_data_sources.clear();
	return;
}

bool USSR_optimizer::useStrings(optional_ptr<LogicalOperator> op) {
	switch (op->type) {

	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr_op = op->Cast<LogicalAggregate>();
		for (auto &expr : aggr_op.groups) {
			if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
				if (bound_colref.return_type == LogicalType::VARCHAR) {
					return true;
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
					return true;
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
					return true;
				}
			}
			if (condition.right->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref = condition.right->Cast<BoundColumnRefExpression>();
				if (bound_colref.return_type == LogicalType::VARCHAR) {
					return true;
				}
			}
			for (auto &type : join_op.types) {
				if (type == LogicalType::VARCHAR) {
						return true;
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
					return true;
				}
			}
		}
		break;
	}
	default:
		break;
	}
	return false;
}

unique_ptr<LogicalOperator> USSR_optimizer::Rewrite(unique_ptr<LogicalOperator> op) {
	op->ResolveOperatorTypes();

	for (idx_t i = 0; i < op->children.size(); ++i) {
		if (op->children[i]->type == LogicalOperatorType::LOGICAL_GET) {
			for (const auto &type : op->children[i]->types) {
				if (type.id() == LogicalTypeId::VARCHAR) {
					candidate_data_sources.push_back(op.get());
					break;
				}
			}
		}
	}

	auto string_usage = useStrings(op.get());
	// Depth-first-search post-order
	for (idx_t i = 0; i < op->children.size(); ++i) {
		op->children[i] = Rewrite(std::move(op->children[i]));
		if (string_usage) {
			choose_operator();
		}
	}

	// if you don't output VARCHAR columns, clear the candidates vector
	//	bool clear_candidates = true;
	//	for (auto &type : op->types) {
	//		if(type == LogicalType::VARCHAR){
	//			clear_candidates = false;
	//			break;
	//		}
	//	}
	//	if(clear_candidates){
	//		candidate_data_sources.clear();
	//	}
	return op;
}

} // namespace duckdb
