#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<MacroFunction> Transformer::TransformMacroFunction(duckdb_libpgquery::PGFunctionDefinition &def) {
	unique_ptr<MacroFunction> macro_func;
	if (def.function) {
		auto expression = TransformExpression(def.function);
		macro_func = make_uniq<ScalarMacroFunction>(std::move(expression));
	} else if (def.query) {
		auto query_node = TransformSelectNode(*def.query);
		macro_func = make_uniq<TableMacroFunction>(std::move(query_node));
	}

	if (!def.params) {
		return macro_func;
	}
	vector<unique_ptr<ParsedExpression>> parameters;
	TransformExpressionList(*def.params, parameters);
	for (auto &param : parameters) {
		Value const_param;
		if (ConstructConstantFromExpression(*param, const_param)) {
			// parameters with default value (must have an alias)
			if (param->alias.empty()) {
				throw ParserException("Invalid parameter: '%s'", param->ToString());
			}
			if (macro_func->default_parameters.find(param->alias) != macro_func->default_parameters.end()) {
				throw ParserException("Duplicate default parameter: '%s'", param->alias);
			}
			auto constructed_constant = make_uniq<ConstantExpression>(std::move(const_param));
			constructed_constant->alias = param->alias;
			macro_func->default_parameters[param->alias] = std::move(constructed_constant);
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
	return macro_func;
}

unique_ptr<CreateStatement> Transformer::TransformCreateFunction(duckdb_libpgquery::PGCreateFunctionStmt &stmt) {
	D_ASSERT(stmt.type == duckdb_libpgquery::T_PGCreateFunctionStmt);
	D_ASSERT(stmt.functions);

	auto result = make_uniq<CreateStatement>();
	auto qname = TransformQualifiedName(*stmt.name);

	vector<unique_ptr<MacroFunction>> macros;
	for (auto c = stmt.functions->head; c != nullptr; c = lnext(c)) {
		auto &function_def = *PGPointerCast<duckdb_libpgquery::PGFunctionDefinition>(c->data.ptr_value);
		macros.push_back(TransformMacroFunction(function_def));
	}
	PivotEntryCheck("macro");

	auto catalog_type =
	    macros[0]->type == MacroType::SCALAR_MACRO ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY;
	auto info = make_uniq<CreateMacroInfo>(catalog_type);
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
	info->macros = std::move(macros);

	result->info = std::move(info);

	return result;
}

} // namespace duckdb
