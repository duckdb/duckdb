#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformResTarget(duckdb_libpgquery::PGResTarget *root) {
	D_ASSERT(root);

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
	D_ASSERT(root);

	auto expr = TransformExpression((duckdb_libpgquery::PGNode *)root->arg);
	if (root->name) {
		expr->alias = string(root->name);
	}
	return expr;
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(duckdb_libpgquery::PGNode *node) {
	if (!node) {
		return nullptr;
	}

	auto stack_checker = StackCheck();

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
	case duckdb_libpgquery::T_PGGroupingFunc:
		return TransformGroupingFunction(reinterpret_cast<duckdb_libpgquery::PGGroupingFunc *>(node));
	case duckdb_libpgquery::T_PGAStar:
		return TransformStarExpression(node);
	default:
		throw NotImplementedException("Expr of type %d not implemented\n", (int)node->type);
	}
}

void Transformer::TransformExpressionList(duckdb_libpgquery::PGList &list,
                                          vector<unique_ptr<ParsedExpression>> &result) {
	for (auto node = list.head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		D_ASSERT(target);

		auto expr = TransformExpression(target);
		D_ASSERT(expr);

		result.push_back(std::move(expr));
	}
}

} // namespace duckdb
