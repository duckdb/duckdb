//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/cross_product_rewrite.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

// TODO: possibly merge into cross product rewrite, lots of overlap?

#pragma once

#include <algorithm>
#include <vector>

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "planner/operator/logical_filter.hpp"

namespace duckdb {
//

static bool CheckEval(LogicalOperator *op, AbstractExpression *expr) {
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto colref = (ColumnRefExpression *)expr;
		if (op->referenced_tables.find(colref->binding.table_index) !=
		    op->referenced_tables.end()) {
			return true;
		}
		return false;
	} else {
		bool can_eval = true;
		for (auto &child : expr->children) {
			bool child_can_eval = CheckEval(op, child.get());
			if (!child_can_eval) {
				can_eval = false;
				break;
			}
		}
		return can_eval;
	}
	return false;
}

static std::unique_ptr<AbstractExpression>
RewritePushdown(std::unique_ptr<AbstractExpression> expr, LogicalOperator *op) {
	assert(op);
	assert(expr->children.size() == 2);

	bool moved = false;

	for (size_t i = 0; i < op->children.size(); i++) {
		// try to see if child ops are interested in this join condition?
		if (op->children[i]->type == LogicalOperatorType::CROSS_PRODUCT ||
		    op->children[i]->type == LogicalOperatorType::JOIN) {
			expr = move(RewritePushdown(move(expr), op->children[i].get()));
			if (!expr) {
				moved = true;
			}
		}
		if (moved) {
			break;
		}

		// this trick needs two child ops
		if (op->children[i]->children.size() != 2) {
			continue;
		}

		// no? well lets try ourselves then

		ssize_t push_index = -1;
		// if both are from the left or both are from the right or
		// are some constant comparision of one side, push selection
		if (CheckEval(op->children[i]->children[0].get(), expr.get())) {
			push_index = 0;
		}

		if (CheckEval(op->children[i]->children[1].get(), expr.get())) {
			push_index = 1;
		}
		if (push_index > -1) {
			// if the child is not yet a filter, make it one
			if (op->children[i]->children[push_index]->type !=
			    LogicalOperatorType::FILTER) {
				auto filter = make_unique<LogicalFilter>();
				filter->AddChild(move(op->children[i]->children[push_index]));
				op->children[i]->children[push_index] = move(filter);
			}
			assert(op->children[i]->children[push_index]->type ==
			       LogicalOperatorType::FILTER);
			// push filter cond
			auto filter_op = reinterpret_cast<LogicalFilter *>(
			    op->children[i]->children[push_index].get());
			filter_op->expressions.push_back(move(expr));
			moved = true;
		}
	}

	if (!moved) {
		return move(expr);
	} else {
		return nullptr;
	}
}

class SelectionPushdownRule : public Rule {
  public:
	SelectionPushdownRule() {
		root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
		    LogicalOperatorType::FILTER);
		root->child_policy = ChildPolicy::ANY;
	}

	std::unique_ptr<LogicalOperator>
	Apply(LogicalOperator &root, std::vector<AbstractOperator> &bindings) {
		auto &filter = (LogicalFilter &)root;
		assert(filter.children.size() == 1);
		// for each filter condition, check if they can be a join condition
		std::vector<std::unique_ptr<AbstractExpression>> new_expressions;

		for (size_t i = 0; i < filter.expressions.size(); i++) {
			auto &expr = filter.expressions[i];

			// only consider comparisions a=b or the like
			if (expr->children.size() == 2 &&
			    expr->type >= ExpressionType::COMPARE_EQUAL &&
			    expr->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {

				auto ex_again = RewritePushdown(move(expr), &root);
				if (ex_again) {
					new_expressions.push_back(move(ex_again));
				}
			} else {
				new_expressions.push_back(move(expr));
			}
		}
		filter.expressions.clear();
		for (auto &ex : new_expressions) {
			filter.expressions.push_back(move(ex));
		}
		if (filter.expressions.size() > 0) {
			return nullptr;
		} else {
			return move(filter.children[0]);
		}
	};
};

} // namespace duckdb
