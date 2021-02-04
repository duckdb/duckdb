#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

using namespace duckdb_libpgquery;

unique_ptr<PragmaStatement> Transformer::TransformPragma(PGNode *node) {
	auto stmt = reinterpret_cast<PGPragmaStmt *>(node);

	auto result = make_unique<PragmaStatement>();
	auto &info = *result->info;

	info.name = stmt->name;
	// parse the arguments, if any
	if (stmt->args) {
		for (auto cell = stmt->args->head; cell != nullptr; cell = cell->next) {
			auto node = reinterpret_cast<PGNode *>(cell->data.ptr_value);
			auto expr = TransformExpression(node);

			if (expr->type == ExpressionType::COMPARE_EQUAL) {
				auto &comp = (ComparisonExpression &)*expr;
				info.named_parameters[comp.left->ToString()] = Value(comp.right->ToString());
			} else if (node->type == T_PGAConst) {
				auto constant = TransformConstant((PGAConst *)node);
				info.parameters.push_back(((ConstantExpression &)*constant).value);
			} else {
				info.parameters.push_back(Value(expr->ToString()));
			}
		}
	}
	// now parse the pragma type
	switch (stmt->kind) {
	case PG_PRAGMA_TYPE_NOTHING:
		if (info.parameters.size() > 0 || info.named_parameters.size() > 0) {
			throw ParserException("PRAGMA statement that is not a call or assignment cannot contain parameters");
		}
		break;
	case PG_PRAGMA_TYPE_ASSIGNMENT:
		if (info.parameters.size() != 1) {
			throw ParserException("PRAGMA statement with assignment should contain exactly one parameter");
		}
		if (info.named_parameters.size() > 0) {
			throw ParserException("PRAGMA statement with assignment cannot have named parameters");
		}
		break;
	case PG_PRAGMA_TYPE_CALL:
		break;
	default:
		throw ParserException("Unknown pragma type");
	}

	return result;
}

} // namespace duckdb
