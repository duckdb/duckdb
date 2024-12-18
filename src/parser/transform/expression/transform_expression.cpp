#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformResTarget(duckdb_libpgquery::PGResTarget &root) {
	auto expr = TransformExpression(root.val);
	if (!expr) {
		return nullptr;
	}
	if (root.name) {
		expr->SetAlias(root.name);
	}
	return expr;
}

unique_ptr<ParsedExpression> Transformer::TransformNamedArg(duckdb_libpgquery::PGNamedArgExpr &root) {

	auto expr = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.arg));
	if (root.name) {
		expr->SetAlias(root.name);
	}
	return expr;
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(duckdb_libpgquery::PGNode &node) {

	auto stack_checker = StackCheck();

	switch (node.type) {
	case duckdb_libpgquery::T_PGColumnRef:
		return TransformColumnRef(PGCast<duckdb_libpgquery::PGColumnRef>(node));
	case duckdb_libpgquery::T_PGAConst:
		return TransformConstant(PGCast<duckdb_libpgquery::PGAConst>(node));
	case duckdb_libpgquery::T_PGAExpr:
		return TransformAExpr(PGCast<duckdb_libpgquery::PGAExpr>(node));
	case duckdb_libpgquery::T_PGFuncCall:
		return TransformFuncCall(PGCast<duckdb_libpgquery::PGFuncCall>(node));
	case duckdb_libpgquery::T_PGBoolExpr:
		return TransformBoolExpr(PGCast<duckdb_libpgquery::PGBoolExpr>(node));
	case duckdb_libpgquery::T_PGTypeCast:
		return TransformTypeCast(PGCast<duckdb_libpgquery::PGTypeCast>(node));
	case duckdb_libpgquery::T_PGCaseExpr:
		return TransformCase(PGCast<duckdb_libpgquery::PGCaseExpr>(node));
	case duckdb_libpgquery::T_PGSubLink:
		return TransformSubquery(PGCast<duckdb_libpgquery::PGSubLink>(node));
	case duckdb_libpgquery::T_PGCoalesceExpr:
		return TransformCoalesce(PGCast<duckdb_libpgquery::PGAExpr>(node));
	case duckdb_libpgquery::T_PGNullTest:
		return TransformNullTest(PGCast<duckdb_libpgquery::PGNullTest>(node));
	case duckdb_libpgquery::T_PGResTarget:
		return TransformResTarget(PGCast<duckdb_libpgquery::PGResTarget>(node));
	case duckdb_libpgquery::T_PGParamRef:
		return TransformParamRef(PGCast<duckdb_libpgquery::PGParamRef>(node));
	case duckdb_libpgquery::T_PGNamedArgExpr:
		return TransformNamedArg(PGCast<duckdb_libpgquery::PGNamedArgExpr>(node));
	case duckdb_libpgquery::T_PGSQLValueFunction:
		return TransformSQLValueFunction(PGCast<duckdb_libpgquery::PGSQLValueFunction>(node));
	case duckdb_libpgquery::T_PGSetToDefault:
		return make_uniq<DefaultExpression>();
	case duckdb_libpgquery::T_PGCollateClause:
		return TransformCollateExpr(PGCast<duckdb_libpgquery::PGCollateClause>(node));
	case duckdb_libpgquery::T_PGIntervalConstant:
		return TransformInterval(PGCast<duckdb_libpgquery::PGIntervalConstant>(node));
	case duckdb_libpgquery::T_PGLambdaFunction:
		return TransformLambda(PGCast<duckdb_libpgquery::PGLambdaFunction>(node));
	case duckdb_libpgquery::T_PGAIndirection:
		return TransformArrayAccess(PGCast<duckdb_libpgquery::PGAIndirection>(node));
	case duckdb_libpgquery::T_PGPositionalReference:
		return TransformPositionalReference(PGCast<duckdb_libpgquery::PGPositionalReference>(node));
	case duckdb_libpgquery::T_PGGroupingFunc:
		return TransformGroupingFunction(PGCast<duckdb_libpgquery::PGGroupingFunc>(node));
	case duckdb_libpgquery::T_PGAStar:
		return TransformStarExpression(PGCast<duckdb_libpgquery::PGAStar>(node));
	case duckdb_libpgquery::T_PGBooleanTest:
		return TransformBooleanTest(PGCast<duckdb_libpgquery::PGBooleanTest>(node));
	case duckdb_libpgquery::T_PGMultiAssignRef:
		return TransformMultiAssignRef(PGCast<duckdb_libpgquery::PGMultiAssignRef>(node));

	default:
		throw NotImplementedException("Expression type %s (%d)", NodetypeToString(node.type), (int)node.type);
	}
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(optional_ptr<duckdb_libpgquery::PGNode> node) {
	if (!node) {
		return nullptr;
	}
	return TransformExpression(*node);
}

void Transformer::TransformExpressionList(duckdb_libpgquery::PGList &list,
                                          vector<unique_ptr<ParsedExpression>> &result) {
	for (auto node = list.head; node != nullptr; node = node->next) {
		auto target = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);

		auto expr = TransformExpression(*target);
		result.push_back(std::move(expr));
	}
}

} // namespace duckdb
