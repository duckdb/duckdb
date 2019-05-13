#include "common/exception.hpp"
#include "parser/expression/default_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformResTarget(ResTarget *root) {
	if (!root) {
		return nullptr;
	}
	auto expr = TransformExpression(root->val);
	if (!expr) {
		return nullptr;
	}
	if (root->name) {
		expr->alias = string(root->name);
	}
	return expr;
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(Node *node) {
	if (!node) {
		return nullptr;
	}

	switch (node->type) {
	case T_ColumnRef:
		return TransformColumnRef(reinterpret_cast<ColumnRef *>(node));
	case T_A_Const:
		return TransformConstant(reinterpret_cast<A_Const *>(node));
	case T_A_Expr:
		return TransformAExpr(reinterpret_cast<A_Expr *>(node));
	case T_FuncCall:
		return TransformFuncCall(reinterpret_cast<FuncCall *>(node));
	case T_BoolExpr:
		return TransformBoolExpr(reinterpret_cast<BoolExpr *>(node));
	case T_TypeCast:
		return TransformTypeCast(reinterpret_cast<TypeCast *>(node));
	case T_CaseExpr:
		return TransformCase(reinterpret_cast<CaseExpr *>(node));
	case T_SubLink:
		return TransformSubquery(reinterpret_cast<SubLink *>(node));
	case T_CoalesceExpr:
		return TransformCoalesce(reinterpret_cast<A_Expr *>(node));
	case T_NullTest:
		return TransformNullTest(reinterpret_cast<NullTest *>(node));
	case T_ResTarget:
		return TransformResTarget(reinterpret_cast<ResTarget *>(node));
	case T_ParamRef:
		return TransformParamRef(reinterpret_cast<ParamRef *>(node));
	case T_SetToDefault:
		return make_unique<DefaultExpression>();

	default:
		throw NotImplementedException("Expr of type %d not implemented\n", (int)node->type);
	}
}

bool Transformer::TransformExpressionList(List *list, vector<unique_ptr<ParsedExpression>> &result) {
	if (!list) {
		return false;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<Node *>(node->data.ptr_value);
		if (!target) {
			return false;
		}
		auto expr = TransformExpression(target);
		if (!expr) {
			return false;
		}
		result.push_back(move(expr));
	}
	return true;
}
