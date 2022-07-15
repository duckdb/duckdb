#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/transformer.hpp"

#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateFunction(duckdb_libpgquery::PGNode *node) {
	D_ASSERT(node);
	D_ASSERT(node->type == duckdb_libpgquery::T_PGCreateFunctionStmt);

	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateFunctionStmt *>(node);
	D_ASSERT(stmt);
	D_ASSERT(stmt->function || stmt->query);

	auto result = make_unique<CreateStatement>();
	auto qname = TransformQualifiedName(stmt->name);

	unique_ptr<MacroFunction> macro_func;

	// function can be null here
	if (stmt->function) {
		auto expression = TransformExpression(stmt->function);
		macro_func = make_unique<ScalarMacroFunction>(move(expression));
	} else if (stmt->query) {
		auto query_node = TransformSelect(stmt->query, true)->node->Copy();
		macro_func = make_unique<TableMacroFunction>(move(query_node));
	}

	auto info =
	    make_unique<CreateMacroInfo>((stmt->function ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY));
	info->schema = qname.schema;
	info->name = qname.name;

	switch (stmt->relpersistence) {
	case duckdb_libpgquery::PG_RELPERSISTENCE_TEMP:
		info->temporary = true;
		break;
	case duckdb_libpgquery::PG_RELPERSISTENCE_UNLOGGED:
		throw ParserException("Unlogged flag not supported for macros: '%s'", qname.name);
		break;
	case duckdb_libpgquery::RELPERSISTENCE_PERMANENT:
		info->temporary = false;
		break;
	}

	if (stmt->params) {
		vector<unique_ptr<ParsedExpression>> parameters;
		TransformExpressionList(*stmt->params, parameters);
		for (auto &param : parameters) {
			if (param->type == ExpressionType::VALUE_CONSTANT) {
				// parameters with default value (must have an alias)
				if (param->alias.empty()) {
					throw ParserException("Invalid parameter: '%s'", param->ToString());
				}
				if (macro_func->default_parameters.find(param->alias) != macro_func->default_parameters.end()) {
					throw ParserException("Duplicate default parameter: '%s'", param->alias);
				}
				macro_func->default_parameters[param->alias] = move(param);
			} else if (param->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				// positional parameters
				if (!macro_func->default_parameters.empty()) {
					throw ParserException("Positional parameters cannot come after parameters with a default value!");
				}
				macro_func->parameters.push_back(move(param));
			} else {
				throw ParserException("Invalid parameter: '%s'", param->ToString());
			}
		}
	}

	info->function = move(macro_func);
	result->info = move(info);

	return result;
}

} // namespace duckdb
