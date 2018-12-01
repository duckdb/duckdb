
#include <algorithm>

#include "common/common.hpp"
#include "common/exception.hpp"

#include "optimizer/logical_rules/selection_pushdown.hpp"

#include "parser/expression/cast_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "planner/logical_operator_visitor.hpp"
#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

static bool CheckEval(LogicalOperator *op, Expression *expr) {
	if (expr->type == ExpressionType::SELECT_SUBQUERY) {
		// don't push down subqueries
		return false;
	}
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

static unique_ptr<Expression> RewritePushdown(unique_ptr<Expression> expr,
                                              LogicalOperator *op) {
	assert(op);

	bool moved = false;

	for (size_t i = 0; i < op->children.size(); i++) {
		// try to see if child ops are interested in this join condition?
		if (op->children[i]->type == LogicalOperatorType::CROSS_PRODUCT ||
		    op->children[i]->type == LogicalOperatorType::JOIN) {
			expr = RewritePushdown(move(expr), op->children[i].get());
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
		return expr;
	} else {
		return nullptr;
	}
}

SelectionPushdownRule::SelectionPushdownRule() {
	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
	    LogicalOperatorType::FILTER);
	root->child_policy = ChildPolicy::ANY;
}

unique_ptr<LogicalOperator>
SelectionPushdownRule::Apply(Rewriter &rewriter, LogicalOperator &root,
                             vector<AbstractOperator> &bindings,
                             bool &fixed_point) {
	auto &filter = (LogicalFilter &)root;
	assert(filter.children.size() == 1);
	// for each filter condition, check if they can be a join condition
	vector<unique_ptr<Expression>> new_expressions;

	for (size_t i = 0; i < filter.expressions.size(); i++) {
		auto &expr = filter.expressions[i];

		auto ex_again = RewritePushdown(move(expr), &root);
		if (ex_again) {
			new_expressions.push_back(move(ex_again));
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
