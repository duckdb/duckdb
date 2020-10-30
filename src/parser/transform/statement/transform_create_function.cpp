#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_sql_function_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<CreateStatement> Transformer::TransformCreateFunction(PGNode *node) {
	assert(node);
	assert(node->type == T_PGCreateFunctionStmt);

	auto stmt = reinterpret_cast<PGCreateFunctionStmt *>(node);
	assert(stmt);
	assert(stmt->function->type == T_PGAExpr);

	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateSQLFunctionInfo>();

	info->name = stmt->name;
	if (stmt->args) {
		vector<unique_ptr<ParsedExpression>> arg_list;
		auto res = TransformExpressionList(stmt->args, arg_list);
		if (!res) {
			throw Exception("Failed to transform function arguments");
		}
		for (auto &arg : arg_list) {
			info->arguments.push_back(move(arg));
		}
	}
	auto function = TransformExpression(stmt->function);
	assert(function);
	info->function = move(function);

	result->info = move(info);
	return result;
}

} // namespace duckdb
