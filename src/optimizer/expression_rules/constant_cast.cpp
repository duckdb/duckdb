
#include <algorithm>
#include <vector>

#include "optimizer/expression_rules/constant_cast.hpp"

#include "common/exception.hpp"
#include "common/internal_types.hpp"

#include "parser/expression/cast_expression.hpp"
#include "parser/expression/constant_expression.hpp"

using namespace duckdb;
using namespace std;

ConstantCastRule::ConstantCastRule() {
	root = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
	    ExpressionType::OPERATOR_CAST);
	root->children.push_back(
	    make_unique_base<AbstractRuleNode, ExpressionNodeType>(
	        ExpressionType::VALUE_CONSTANT));
	root->child_policy = ChildPolicy::UNORDERED;
}

unique_ptr<Expression>
ConstantCastRule::Apply(Rewriter &rewriter, Expression &root,
                        vector<AbstractOperator> &bindings, bool &fixed_point) {
	auto &cast_expr = (CastExpression &)root;
	auto const_expr =
	    reinterpret_cast<ConstantExpression *>(root.children[0].get());
	return make_unique<ConstantExpression>(
	    const_expr->value.CastAs(cast_expr.return_type));
};
