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
#include "parser/expression/expression_list.hpp"
#include "planner/operator/logical_list.hpp"

namespace duckdb {

class SubqueryRewritingRule : public Rule {
  public:
	SubqueryRewritingRule() {
		auto subquery = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
		    ExpressionType::SELECT_SUBQUERY);

		auto equal = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
		    ExpressionType::COMPARE_EQUAL);

		equal->children.push_back(move(subquery));
		equal->child_policy = ChildPolicy::SOME;

		root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
		    LogicalOperatorType::FILTER);

		root->children.push_back(move(equal));
		root->child_policy = ChildPolicy::SOME;
	}

	std::unique_ptr<LogicalOperator>
	Apply(LogicalOperator &op_root, std::vector<AbstractOperator> &bindings) {

		auto *filter = (LogicalFilter *)bindings[0].value.op;
		auto *equal = (ComparisonExpression *)bindings[1].value.expr;
		auto *subquery = (SubqueryExpression *)bindings[2].value.expr;

		// subquery->op->Print();

		// step 1: check if there is a correlation in the subquery
		if (!subquery->is_correlated) {
			return nullptr;
		}

		// step 2: check that subquery is aggr
		if (subquery->op->type != LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			throw Exception("Expected aggregation here!");
		}

		// step 3: find correlation
		// subquery->op->Print();

		ColumnRefExpression *corcol = nullptr;

		//	static bool MatchOperands(AbstractRuleNode *node, AbstractOperator
		// rel,
		//      std::vector<AbstractOperator> &bindings);

		auto sq_colref_local =
		    make_unique_base<AbstractRuleNode, ColumnRefNodeDepth>(0);
		auto sq_colref_upper =
		    make_unique_base<AbstractRuleNode, ColumnRefNodeDepth>(1);
		auto sq_eq = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
		    ExpressionType::COMPARE_EQUAL);

		sq_eq->children.push_back(move(sq_colref_local));
		sq_eq->children.push_back(move(sq_colref_upper));

		sq_eq->child_policy = ChildPolicy::SOME;

		auto sq_root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
		    LogicalOperatorType::FILTER);

		sq_root->children.push_back(move(sq_eq));
		sq_root->child_policy = ChildPolicy::SOME;

		std::vector<AbstractOperator> sq_bindings;

		// TODO use the matcher for this
		auto aop = AbstractOperator(subquery->op.get());
		for (auto it = aop.begin(); it != aop.end(); it++) {
			// it->Print();
			if (Rewriter::MatchOperands(sq_root.get(), *it, sq_bindings)) {
				break;
			}
		}
		if (sq_bindings.size() == 0) {
			throw Exception("Could not find equality correlation comparision");
		}

		auto *sq_filter = (LogicalFilter *)sq_bindings[0].value.op;
		auto *sq_comp = (ComparisonExpression *)sq_bindings[1].value.expr;
		auto *sq_colref_inner =
		    (ColumnRefExpression *)sq_bindings[2].value.expr;
		auto *sq_colref_outer =
		    (ColumnRefExpression *)sq_bindings[3].value.expr;

		auto comp_left =
		    make_unique_base<AbstractExpression, ColumnRefExpression>(
		        sq_colref_inner->return_type, sq_colref_inner->binding);
		auto comp_right =
		    make_unique_base<AbstractExpression, ColumnRefExpression>(
		        sq_colref_outer->return_type, sq_colref_outer->binding);

		auto aggr = (LogicalAggregate *)subquery->op.get();

		// FIXME uuugly
		size_t child_index = 0;
		if (sq_comp->children[0].get() == sq_bindings[3].value.expr) {
			child_index = 1;
		}

		auto colref_inner = move(sq_comp->children[child_index]);

		// install a as both projection and grouping col in subquery
		aggr->expressions.push_back(
		    make_unique_base<AbstractExpression, GroupRefExpression>(
		        colref_inner->return_type, aggr->groups.size()));
		aggr->groups.push_back(move(colref_inner));

		// FIXME this assumes only one child
		sq_filter->expressions.clear();

		// subquery->op->Print();

		assert(filter->children.size() == 1);

		// add join
		auto join =
		    make_unique_base<LogicalOperator, LogicalJoin>(JoinType::INNER);
		join->children.push_back(move(filter->children[0]));
		join->children.push_back(move(subquery->op));

		auto comp = make_unique_base<AbstractExpression, ComparisonExpression>(
		    ExpressionType::COMPARE_EQUAL, move(comp_left), move(comp_right));
		join->expressions.push_back(move(comp));

		filter->children[0] = move(join);
		// filter->Print();

		return nullptr;

		//	throw Exception("sq cross product rewrite!");
		// check if there's any
	};
};

} // namespace duckdb
