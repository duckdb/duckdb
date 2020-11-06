#include "duckdb/parser/parsed_data/create_macro_function_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/function/macro_function.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<CreateStatement> Transformer::TransformCreateFunction(PGNode *node) {
	D_ASSERT(node);
    D_ASSERT(node->type == T_PGCreateFunctionStmt);

	auto stmt = reinterpret_cast<PGCreateFunctionStmt *>(node);
    D_ASSERT(stmt);

	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateMacroFunctionInfo>();

	auto qname = TransformQualifiedName(stmt->name);
	info->schema = qname.schema;
	info->name = qname.name;

	auto function = TransformExpression(stmt->function);
    D_ASSERT(function);
	auto macro_func = make_unique<MacroFunction>(move(function));

	if (stmt->args) {
		vector<unique_ptr<ParsedExpression>> arg_list;
		auto res = TransformExpressionList(stmt->args, arg_list);
		if (!res) {
			throw Exception("Failed to transform function arguments");
		}
		for (auto &arg : arg_list) {
			macro_func->arguments.push_back(move(arg));
		}
	}

	info->function = move(macro_func);
	result->info = move(info);
	return result;
}

} // namespace duckdb
