#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformResTarget(duckdb_libpgquery::PGResTarget *root) {
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

unique_ptr<ParsedExpression> Transformer::TransformNamedArg(duckdb_libpgquery::PGNamedArgExpr *root) {
	if (!root) {
		return nullptr;
	}
	auto expr = TransformExpression((duckdb_libpgquery::PGNode *)root->arg);
	if (root->name) {
		expr->alias = string(root->name);
	}
	return expr;
}

void Transformer::TransformExpressionLazy(duckdb_libpgquery::PGNode *node, unique_ptr<ParsedExpression> &result) {
	if (!node) {
		return;
	}
	expressions_to_be_transformed.push_back(ExpressionTransformTarget(node, result));
}

unique_ptr<ParsedExpression> Transformer::TransformExpressionInternal(duckdb_libpgquery::PGNode *node) {
	if (!node) {
		return nullptr;
	}

	switch (node->type) {
	case duckdb_libpgquery::T_PGColumnRef:
		return TransformColumnRef(reinterpret_cast<duckdb_libpgquery::PGColumnRef *>(node));
	case duckdb_libpgquery::T_PGAConst:
		return TransformConstant(reinterpret_cast<duckdb_libpgquery::PGAConst *>(node));
	case duckdb_libpgquery::T_PGAExpr:
		return TransformAExpr(reinterpret_cast<duckdb_libpgquery::PGAExpr *>(node));
	case duckdb_libpgquery::T_PGFuncCall:
		return TransformFuncCall(reinterpret_cast<duckdb_libpgquery::PGFuncCall *>(node));
	case duckdb_libpgquery::T_PGBoolExpr:
		return TransformBoolExpr(reinterpret_cast<duckdb_libpgquery::PGBoolExpr *>(node));
	case duckdb_libpgquery::T_PGTypeCast:
		return TransformTypeCast(reinterpret_cast<duckdb_libpgquery::PGTypeCast *>(node));
	case duckdb_libpgquery::T_PGCaseExpr:
		return TransformCase(reinterpret_cast<duckdb_libpgquery::PGCaseExpr *>(node));
	case duckdb_libpgquery::T_PGSubLink:
		return TransformSubquery(reinterpret_cast<duckdb_libpgquery::PGSubLink *>(node));
	case duckdb_libpgquery::T_PGCoalesceExpr:
		return TransformCoalesce(reinterpret_cast<duckdb_libpgquery::PGAExpr *>(node));
	case duckdb_libpgquery::T_PGNullTest:
		return TransformNullTest(reinterpret_cast<duckdb_libpgquery::PGNullTest *>(node));
	case duckdb_libpgquery::T_PGResTarget:
		return TransformResTarget(reinterpret_cast<duckdb_libpgquery::PGResTarget *>(node));
	case duckdb_libpgquery::T_PGParamRef:
		return TransformParamRef(reinterpret_cast<duckdb_libpgquery::PGParamRef *>(node));
	case duckdb_libpgquery::T_PGNamedArgExpr:
		return TransformNamedArg(reinterpret_cast<duckdb_libpgquery::PGNamedArgExpr *>(node));
	case duckdb_libpgquery::T_PGSQLValueFunction:
		return TransformSQLValueFunction(reinterpret_cast<duckdb_libpgquery::PGSQLValueFunction *>(node));
	case duckdb_libpgquery::T_PGSetToDefault:
		return make_unique<DefaultExpression>();
	case duckdb_libpgquery::T_PGCollateClause:
		return TransformCollateExpr(reinterpret_cast<duckdb_libpgquery::PGCollateClause *>(node));
	case duckdb_libpgquery::T_PGIntervalConstant:
		return TransformInterval(reinterpret_cast<duckdb_libpgquery::PGIntervalConstant *>(node));
	case duckdb_libpgquery::T_PGLambdaFunction:
		return TransformLambda(reinterpret_cast<duckdb_libpgquery::PGLambdaFunction *>(node));
	case duckdb_libpgquery::T_PGAIndirection:
		return TransformArrayAccess(reinterpret_cast<duckdb_libpgquery::PGAIndirection *>(node));
	case duckdb_libpgquery::T_PGPositionalReference:
		return TransformPositionalReference(reinterpret_cast<duckdb_libpgquery::PGPositionalReference *>(node));
	default:
		throw NotImplementedException("Expr of type %d not implemented\n", (int)node->type);
	}
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(duckdb_libpgquery::PGNode *node) {
	auto result = TransformExpressionInternal(node);
	while(!expressions_to_be_transformed.empty()) {
		auto entry = expressions_to_be_transformed.back();
		expressions_to_be_transformed.pop_back();
		entry.result = TransformExpressionInternal(entry.node);
	}
	return result;
}

bool Transformer::TransformExpressionList(duckdb_libpgquery::PGList *list,
                                          vector<unique_ptr<ParsedExpression>> &result) {
	if (!list) {
		return false;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
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

} // namespace duckdb
