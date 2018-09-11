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
		if (root.children[0]->type != LogicalOperatorType::CROSS_PRODUCT) {
			return nullptr;
		}
		// for each filter condition, check if they can be a join condition
		std::vector<std::unique_ptr<AbstractExpression>> new_expressions;

		for (size_t i = 0; i < filter.expressions.size(); i++) {
			bool zapped = false;
			auto &ex = filter.expressions[i];
			if (ex->children.size() == 2 &&
			    ex->type >= ExpressionType::COMPARE_EQUAL &&
			    ex->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				size_t left_side = LogicalJoin::GetJoinSide(
				    root.children[0].get(), ex->children[0]);
				size_t right_side = LogicalJoin::GetJoinSide(
				    root.children[0].get(), ex->children[1]);

				if ((left_side == JoinSide::LEFT &&
				     right_side == JoinSide::RIGHT) ||
				    (left_side == JoinSide::RIGHT &&
				     right_side == JoinSide::LEFT)) {
					zapped = true;
					// if crossprod is still not a join, convert
					if (root.children[0]->type ==
					    LogicalOperatorType::CROSS_PRODUCT) {
						auto join = make_unique<LogicalJoin>(JoinType::INNER);
						join->AddChild(move(root.children[0]->children[0]));
						join->AddChild(move(root.children[0]->children[1]));
						root.children[0] = move(join);
					}
					assert(root.children[0]->type == LogicalOperatorType::JOIN);

					auto join_op =
					    reinterpret_cast<LogicalJoin *>(root.children[0].get());
					// push condition into join and remove from filter
					join_op->SetJoinCondition(move(ex));
				}
			}
			// other comparisions need to stick around in the filter
			if (!zapped) {
				new_expressions.push_back(move(ex));
			}
		}
		filter.expressions.clear();
		for (auto &ex : new_expressions) {
			filter.expressions.push_back(move(ex));
		}

		// TODO: find all filter children that are also crossprods and try with
		// the filter and them, too
		if (filter.expressions.size() > 0) {
			return nullptr;
		} else {
			return move(filter.children[0]);
		}
	};
};

} // namespace duckdb
