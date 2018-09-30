//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/cross_product_rewrite.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_join.hpp"

namespace duckdb {

static std::unique_ptr<Expression> RewriteCP(std::unique_ptr<Expression> expr,
                                             LogicalOperator *op);

// TODO, this passing ex back and forth is kind of annoying. better ideas?
// we start with op being the parent filter which contains a crossprod or join
// which in turn contain other crossprods or joins
static std::unique_ptr<Expression> RewriteCP(std::unique_ptr<Expression> expr,
                                             LogicalOperator *op) {
	assert(op);
	assert(expr->children.size() == 2);

	bool moved = false;

	for (size_t i = 0; i < op->children.size(); i++) {
		// try to see if child ops are interested in this join condition?
		if (op->children[i]->type == LogicalOperatorType::CROSS_PRODUCT ||
		    op->children[i]->type == LogicalOperatorType::JOIN) {
			expr = RewriteCP(move(expr), op->children[i].get());
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
		// NB: 'children' mean different things for op and expr
		JoinSide left_side =
		    LogicalJoin::GetJoinSide(op->children[i].get(), expr->children[0]);
		JoinSide right_side =
		    LogicalJoin::GetJoinSide(op->children[i].get(), expr->children[1]);

		if ((left_side == JoinSide::LEFT && right_side == JoinSide::RIGHT) ||
		    (left_side == JoinSide::RIGHT && right_side == JoinSide::LEFT)) {
			// if crossprod is still not a join, convert
			if (op->children[i]->type == LogicalOperatorType::CROSS_PRODUCT) {
				auto join = make_unique<LogicalJoin>(JoinType::INNER);
				join->AddChild(move(op->children[i]->children[0]));
				join->AddChild(move(op->children[i]->children[1]));
				op->children[i] = move(join);
			}
			assert(op->children[i]->type == LogicalOperatorType::JOIN);

			auto join_op =
			    reinterpret_cast<LogicalJoin *>(op->children[i].get());
			// push condition into join and remove from filter
			join_op->SetJoinCondition(move(expr));
			moved = true;
		}
	}

	if (!moved) {
		return expr;
	} else {
		return nullptr;
	}
}

// TODO: this rule would be simplified by a matcher of filter ... any chain of
// children of type crossprod or join ... crosprod then we could apply multiple
// times instead of this mess

class CrossProductRewrite : public Rule {
  public:
	CrossProductRewrite() {
		root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
		    LogicalOperatorType::FILTER);
		root->children.push_back(
		    make_unique_base<AbstractRuleNode, LogicalNodeType>(
		        LogicalOperatorType::CROSS_PRODUCT));
		root->child_policy = ChildPolicy::SOME;
	}

	std::unique_ptr<LogicalOperator>
	Apply(LogicalOperator &root, std::vector<AbstractOperator> &bindings) {
		auto &filter = (LogicalFilter &)root;
		assert(filter.children.size() == 1);
		// for each filter condition, check if they can be a join condition
		std::vector<std::unique_ptr<Expression>> new_expressions;

		for (size_t i = 0; i < filter.expressions.size(); i++) {
			auto &expr = filter.expressions[i];

			// only consider comparisions a=b or the like
			if (expr->children.size() == 2 &&
			    expr->type >= ExpressionType::COMPARE_EQUAL &&
			    expr->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {

				auto ex_again = RewriteCP(move(expr), &root);
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
