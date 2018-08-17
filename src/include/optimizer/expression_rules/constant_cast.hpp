//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/expression_rules/constant_cast.hpp
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

namespace duckdb {

class ConstantCastRule : public Rule {
  public:
	ConstantCastRule() {
		root = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
		    ExpressionType::OPERATOR_CAST);
		root->children.push_back(
		    make_unique_base<AbstractRuleNode, ExpressionNodeType>(
		        ExpressionType::VALUE_CONSTANT));
		root->child_policy = ChildPolicy::UNORDERED;
	}

	std::unique_ptr<AbstractExpression>
	Apply(AbstractExpression &root, std::vector<AbstractOperator *> &bindings) {

		auto &cast_expr = (CastExpression &)root;
		auto const_expr =
		    reinterpret_cast<ConstantExpression *>(root.children[0].get());
		return make_unique<ConstantExpression>(
		    const_expr->value.CastAs(cast_expr.return_type));
	};
};

} // namespace duckdb
