#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformResTarget(PGResTarget *root) {
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

unique_ptr<ParsedExpression> Transformer::TransformNamedArg(PGNamedArgExpr *root) {
	if (!root) {
		return nullptr;
	}
	auto expr = TransformExpression((PGNode *)root->arg);
	if (root->name) {
		expr->alias = string(root->name);
	}
	return expr;
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(PGNode *node) {
	if (!node) {
		return nullptr;
	}

	switch (node->type) {
	case T_PGColumnRef:
		return TransformColumnRef(reinterpret_cast<PGColumnRef *>(node));
	case T_PGAConst:
		return TransformConstant(reinterpret_cast<PGAConst *>(node));
	case T_PGAExpr:
		return TransformAExpr(reinterpret_cast<PGAExpr *>(node));
	case T_PGFuncCall:
		return TransformFuncCall(reinterpret_cast<PGFuncCall *>(node));
	case T_PGBoolExpr:
		return TransformBoolExpr(reinterpret_cast<PGBoolExpr *>(node));
	case T_PGTypeCast:
		return TransformTypeCast(reinterpret_cast<PGTypeCast *>(node));
	case T_PGCaseExpr:
		return TransformCase(reinterpret_cast<PGCaseExpr *>(node));
	case T_PGSubLink:
		return TransformSubquery(reinterpret_cast<PGSubLink *>(node));
	case T_PGCoalesceExpr:
		return TransformCoalesce(reinterpret_cast<PGAExpr *>(node));
	case T_PGNullTest:
		return TransformNullTest(reinterpret_cast<PGNullTest *>(node));
	case T_PGResTarget:
		return TransformResTarget(reinterpret_cast<PGResTarget *>(node));
	case T_PGParamRef:
		return TransformParamRef(reinterpret_cast<PGParamRef *>(node));
	case T_PGNamedArgExpr:
		return TransformNamedArg(reinterpret_cast<PGNamedArgExpr *>(node));
	case T_PGSQLValueFunction:
		return TransformSQLValueFunction(reinterpret_cast<PGSQLValueFunction *>(node));
	case T_PGSetToDefault:
		return make_unique<DefaultExpression>();
	case T_PGCollateClause:
		return TransformCollateExpr(reinterpret_cast<PGCollateClause*>(node));
	default:
		throw NotImplementedException("Expr of type %d not implemented\n", (int)node->type);
	}
}

bool Transformer::TransformExpressionList(PGList *list, vector<unique_ptr<ParsedExpression>> &result) {
	if (!list) {
		return false;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<PGNode *>(node->data.ptr_value);
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
