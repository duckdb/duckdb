#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PragmaStatement> Transformer::TransformPragma(PGNode *node) {
	auto stmt = reinterpret_cast<PGPragmaStmt *>(node);

	auto result = make_unique<PragmaStatement>();
	auto &info = *result->info;

	info.name = stmt->name;
	// parse the arguments, if any
	if (stmt->args) {
		for (auto cell = stmt->args->head; cell != nullptr; cell = cell->next) {
			auto node = reinterpret_cast<PGNode *>(cell->data.ptr_value);
			if (node->type != T_PGAConst) {
				throw ParserException("Unsupported PRAGMA parameter: can only accept constants!");
			}
			auto constant = TransformConstant((PGAConst *)node);
			info.parameters.push_back(((ConstantExpression &)*constant).value);
		}
	}
	// now parse the pragma type
	switch (stmt->kind) {
	case PG_PRAGMA_TYPE_NOTHING:
		if (info.parameters.size() > 0) {
			throw ParserException("PRAGMA statement that is not a call or assignment cannot contain parameters");
		}
		info.pragma_type = PragmaType::NOTHING;
		break;
	case PG_PRAGMA_TYPE_ASSIGNMENT:
		if (info.parameters.size() != 1) {
			throw ParserException("PRAGMA statement with assignment should contain exactly one parameter");
		}
		info.pragma_type = PragmaType::ASSIGNMENT;
		break;
	case PG_PRAGMA_TYPE_CALL:
		info.pragma_type = PragmaType::CALL;
		break;
	default:
		throw ParserException("Unknown pragma type");
	}

	return result;
}
