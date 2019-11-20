#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformResTarget(postgres::PGResTarget *root) {
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

unique_ptr<ParsedExpression> Transformer::TransformExpression(postgres::PGNode *node) {
	if (!node) {
		return nullptr;
	}

	switch (node->type) {
	case postgres::T_PGColumnRef:
		return TransformColumnRef(reinterpret_cast<postgres::PGColumnRef *>(node));
	case postgres::T_PGAConst:
		return TransformConstant(reinterpret_cast<postgres::PGAConst *>(node));
	case postgres::T_PGAExpr:
		return TransformAExpr(reinterpret_cast<postgres::PGAExpr *>(node));
	case postgres::T_PGFuncCall:
		return TransformFuncCall(reinterpret_cast<postgres::PGFuncCall *>(node));
	case postgres::T_PGBoolExpr:
		return TransformBoolExpr(reinterpret_cast<postgres::PGBoolExpr *>(node));
	case postgres::T_PGTypeCast:
		return TransformTypeCast(reinterpret_cast<postgres::PGTypeCast *>(node));
	case postgres::T_PGCaseExpr:
		return TransformCase(reinterpret_cast<postgres::PGCaseExpr *>(node));
	case postgres::T_PGSubLink:
		return TransformSubquery(reinterpret_cast<postgres::PGSubLink *>(node));
	case postgres::T_PGCoalesceExpr:
		return TransformCoalesce(reinterpret_cast<postgres::PGAExpr *>(node));
	case postgres::T_PGNullTest:
		return TransformNullTest(reinterpret_cast<postgres::PGNullTest *>(node));
	case postgres::T_PGResTarget:
		return TransformResTarget(reinterpret_cast<postgres::PGResTarget *>(node));
	case postgres::T_PGParamRef:
		return TransformParamRef(reinterpret_cast<postgres::PGParamRef *>(node));
	case postgres::T_PGSQLValueFunction:
		return TransformSQLValueFunction(reinterpret_cast<postgres::PGSQLValueFunction *>(node));
	case postgres::T_PGSetToDefault:
		return make_unique<DefaultExpression>();

	default:
		throw NotImplementedException("Expr of type %d not implemented\n", (int)node->type);
	}
}

bool Transformer::TransformExpressionList(postgres::PGList *list, vector<unique_ptr<ParsedExpression>> &result) {
	if (!list) {
		return false;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<postgres::PGNode *>(node->data.ptr_value);
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
