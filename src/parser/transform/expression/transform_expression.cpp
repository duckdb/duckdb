#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformResTarget(postgres::ResTarget *root) {
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

unique_ptr<ParsedExpression> Transformer::TransformExpression(postgres::Node *node) {
	if (!node) {
		return nullptr;
	}

	switch (node->type) {
	case postgres::T_ColumnRef:
		return TransformColumnRef(reinterpret_cast<postgres::ColumnRef *>(node));
	case postgres::T_A_Const:
		return TransformConstant(reinterpret_cast<postgres::A_Const *>(node));
	case postgres::T_A_Expr:
		return TransformAExpr(reinterpret_cast<postgres::A_Expr *>(node));
	case postgres::T_FuncCall:
		return TransformFuncCall(reinterpret_cast<postgres::FuncCall *>(node));
	case postgres::T_BoolExpr:
		return TransformBoolExpr(reinterpret_cast<postgres::BoolExpr *>(node));
	case postgres::T_TypeCast:
		return TransformTypeCast(reinterpret_cast<postgres::TypeCast *>(node));
	case postgres::T_CaseExpr:
		return TransformCase(reinterpret_cast<postgres::CaseExpr *>(node));
	case postgres::T_SubLink:
		return TransformSubquery(reinterpret_cast<postgres::SubLink *>(node));
	case postgres::T_CoalesceExpr:
		return TransformCoalesce(reinterpret_cast<postgres::A_Expr *>(node));
	case postgres::T_NullTest:
		return TransformNullTest(reinterpret_cast<postgres::NullTest *>(node));
	case postgres::T_ResTarget:
		return TransformResTarget(reinterpret_cast<postgres::ResTarget *>(node));
	case postgres::T_ParamRef:
		return TransformParamRef(reinterpret_cast<postgres::ParamRef *>(node));
	case postgres::T_SQLValueFunction:
		return TransformSQLValueFunction(reinterpret_cast<postgres::SQLValueFunction *>(node));
	case postgres::T_SetToDefault:
		return make_unique<DefaultExpression>();

	default:
		throw NotImplementedException("Expr of type %d not implemented\n", (int)node->type);
	}
}

bool Transformer::TransformExpressionList(postgres::List *list, vector<unique_ptr<ParsedExpression>> &result) {
	if (!list) {
		return false;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<postgres::Node *>(node->data.ptr_value);
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
