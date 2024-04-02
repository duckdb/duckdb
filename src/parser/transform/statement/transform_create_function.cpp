#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/transformer.hpp"

#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateFunction(duckdb_libpgquery::PGCreateFunctionStmt &stmt) {
	D_ASSERT(stmt.type == duckdb_libpgquery::T_PGCreateFunctionStmt);
	D_ASSERT(stmt.function || stmt.query);

	auto result = make_uniq<CreateStatement>();
	auto qname = TransformQualifiedName(*stmt.name);

	unique_ptr<MacroFunction> macro_func;

	// function can be null here
	if (stmt.function) {
		auto expression = TransformExpression(stmt.function);
		macro_func = make_uniq<ScalarMacroFunction>(std::move(expression));
	} else if (stmt.query) {
		auto query_node =
		    TransformSelect(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(stmt.query), true)->node->Copy();
		macro_func = make_uniq<TableMacroFunction>(std::move(query_node));
	}
	PivotEntryCheck("macro");

	auto info = make_uniq<CreateMacroInfo>(stmt.function ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY);
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->name = qname.name;

	// temporary macro
	switch (stmt.name->relpersistence) {
	case duckdb_libpgquery::PG_RELPERSISTENCE_TEMP:
		info->temporary = true;
		break;
	case duckdb_libpgquery::PG_RELPERSISTENCE_UNLOGGED:
		throw ParserException("Unlogged flag not supported for macros: '%s'", qname.name);
	case duckdb_libpgquery::RELPERSISTENCE_PERMANENT:
		info->temporary = false;
		break;
	default:
		throw ParserException("Unsupported persistence flag for table '%s'", qname.name);
	}

	// what to do on conflict
	info->on_conflict = TransformOnConflict(stmt.onconflict);

	if (stmt.params) {
		vector<unique_ptr<ParsedExpression>> parameters;
		TransformExpressionList(*stmt.params, parameters);
		for (auto &param : parameters) {
			if (param->type == ExpressionType::VALUE_CONSTANT) {
				// parameters with default value (must have an alias)
				if (param->alias.empty()) {
					throw ParserException("Invalid parameter: '%s'", param->ToString());
				}
				if (macro_func->default_parameters.find(param->alias) != macro_func->default_parameters.end()) {
					throw ParserException("Duplicate default parameter: '%s'", param->alias);
				}
				macro_func->default_parameters[param->alias] = std::move(param);
			} else if (param->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				// positional parameters
				if (!macro_func->default_parameters.empty()) {
					throw ParserException("Positional parameters cannot come after parameters with a default value!");
				}
				macro_func->parameters.push_back(std::move(param));
			} else {
				throw ParserException("Invalid parameter: '%s'", param->ToString());
			}
		}
	}

	info->function = std::move(macro_func);
	result->info = std::move(info);

	return result;
}

} // namespace duckdb
