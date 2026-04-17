#include <charconv>

#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "parser/parser.hpp"

namespace duckdb {

// Replace $N positional parameter references with named column references.
// PG uses $1, $2 in function bodies; DuckDB macros use param names directly.
static void ReplacePositionalParams(unique_ptr<ParsedExpression> &expr, const vector<string> &param_names) {
	if (!expr) {
		return;
	}
	if (expr->GetExpressionClass() == ExpressionClass::PARAMETER) {
		auto &param = expr->Cast<ParameterExpression>();
		// Positional params have numeric identifiers: "1", "2", etc.
		idx_t idx = 0;
		auto [ptr, ec] =
		    std::from_chars(param.identifier.data(), param.identifier.data() + param.identifier.size(), idx);
		if (ec != std::errc() || ptr != param.identifier.data() + param.identifier.size()) {
			return;
		}
		if (idx >= 1 && idx <= param_names.size()) {
			auto replacement = make_uniq<ColumnRefExpression>(param_names[idx - 1]);
			replacement->alias = expr->alias;
			expr = std::move(replacement);
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplacePositionalParams(child, param_names); });
}

static void ReplacePositionalParamsInQuery(QueryNode &node, const vector<string> &param_names) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { ReplacePositionalParams(child, param_names); },
	    [&](TableRef &ref) {
		    ParsedExpressionIterator::EnumerateTableRefChildren(
		        ref, [&](unique_ptr<ParsedExpression> &child) { ReplacePositionalParams(child, param_names); });
	    });
}

// Convenient helper for simulating 'try/catch/finally' semantic
template <typename Func>
class [[nodiscard]] Finally {
public:
	static_assert(std::is_nothrow_invocable_v<Func>);

	// If you need some of it, please use absl::Cleanup
	Finally(Finally &&) = delete;
	Finally(const Finally &) = delete;
	Finally &operator=(Finally &&) = delete;
	Finally &operator=(const Finally &) = delete;

	explicit Finally(Func &&func) : _func {std::move(func)} {
	}

	~Finally() noexcept {
		_func();
	}

private:
	[[no_unique_address]] Func _func;
};

unique_ptr<MacroFunction> Transformer::TransformMacroFunction(duckdb_libpgquery::PGFunctionDefinition &def,
                                                              bool has_language) {
	auto saved_named_param_map = std::exchange(named_param_map, {});
	auto saved_last_param_type = std::exchange(last_param_type, PreparedParamType::INVALID);
	Finally restore([&] noexcept {
		named_param_map = std::move(saved_named_param_map);
		last_param_type = saved_last_param_type;
	});
	unique_ptr<MacroFunction> macro_func;
	if (def.function) {
		// When LANGUAGE SQL is specified, the body string is SQL to be parsed.
		// Without LANGUAGE, it's a regular DuckDB macro expression (AS 'string' returns the string).
		if (has_language && def.function->type == duckdb_libpgquery::T_PGAConst) {
			auto &constant = PGCast<duckdb_libpgquery::PGAConst>(*def.function);
			if (constant.val.type == duckdb_libpgquery::T_PGString) {
				auto body_stmts = duckdb_libpgquery::raw_parser(constant.val.val.str);
				if (!body_stmts || body_stmts->length != 1) {
					throw ParserException("Function body must contain exactly one statement");
				}
				auto &raw = PGCast<duckdb_libpgquery::PGRawStmt>(
				    *static_cast<duckdb_libpgquery::PGNode *>(body_stmts->head->data.ptr_value));
				if (raw.stmt->type == duckdb_libpgquery::T_PGSelectStmt) {
					auto query_node = TransformSelectNode(*raw.stmt);
					macro_func = make_uniq<TableMacroFunction>(std::move(query_node));
				} else {
					throw ParserException("Only SELECT statements are supported in SQL function/procedure bodies");
				}
			}
		}
		if (!macro_func) {
			auto expression = TransformExpression(def.function);
			macro_func = make_uniq<ScalarMacroFunction>(std::move(expression));
		}
	} else if (def.query) {
		auto query_node = TransformSelectNode(*def.query);
		macro_func = make_uniq<TableMacroFunction>(std::move(query_node));
	}

	// Apply RETURNS TABLE column aliases to the query's SELECT list
	if (def.returns_table_columns && macro_func && macro_func->type == MacroType::TABLE_MACRO) {
		auto &table_macro = macro_func->Cast<TableMacroFunction>();
		if (table_macro.query_node && table_macro.query_node->type == QueryNodeType::SELECT_NODE) {
			auto &select = table_macro.query_node->Cast<SelectNode>();
			idx_t col_idx = 0;
			for (auto cell = def.returns_table_columns->head; cell && col_idx < select.select_list.size();
			     cell = cell->next, col_idx++) {
				auto &col_def = PGCast<duckdb_libpgquery::PGColumnDef>(
				    *static_cast<duckdb_libpgquery::PGNode *>(cell->data.ptr_value));
				if (col_def.colname) {
					select.select_list[col_idx]->alias = col_def.colname;
				}
			}
		}
	}

	if (!def.params) {
		return macro_func;
	}

	case_insensitive_set_t parameter_names;
	for (auto node = def.params->head; node != nullptr; node = node->next) {
		auto target = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
		if (target->type != duckdb_libpgquery::T_PGFunctionParameter) {
			throw InternalException("TODO");
		}
		auto &param = PGCast<duckdb_libpgquery::PGFunctionParameter>(*target);

		// Bare Typename without name or default: ambiguous case.
		// PG (has_language): unnamed positional param — keep type, generate $N name.
		// DuckDB (!has_language): bare param name parsed as GenericType — extract name, clear type.
		string param_name;
		if (param.ambiguous) {
			// Bare Typename: could be DuckDB name or PG unnamed type.
			// Grammar stored both name (from Typename string) and typeName.
			if (has_language) {
				// PG: use as type, generate positional name
				param_name = "$" + to_string(macro_func->parameters.size() + 1);
			} else {
				// DuckDB: use as name, clear type
				param_name = param.name;
				param.typeName = nullptr;
			}
		} else {
			param_name = param.name ? param.name : ("$" + to_string(macro_func->parameters.size() + 1));
		}

		// Transform parameter name/type
		if (!parameter_names.insert(param_name).second) {
			throw ParserException("Duplicate parameter '%s' in macro definition", param_name);
		}
		macro_func->parameters.emplace_back(make_uniq<ColumnRefExpression>(param_name));
		macro_func->types.emplace_back(param.typeName ? TransformTypeName(*param.typeName) : LogicalType::UNKNOWN);

		// Transform parameter default value
		if (param.defaultValue) {
			auto default_expr = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(param.defaultValue));
			default_expr->SetAlias(param.name);
			macro_func->default_parameters[param.name] = std::move(default_expr);
		} else if (!macro_func->default_parameters.empty()) {
			throw ParserException("Parameter without a default follows parameter with a default");
		}
	}

	// Replace $N positional params with named params in the function body
	if (!macro_func->parameters.empty()) {
		vector<string> param_name_list;
		for (auto &p : macro_func->parameters) {
			param_name_list.push_back(p->Cast<ColumnRefExpression>().GetColumnName());
		}
		if (macro_func->type == MacroType::SCALAR_MACRO) {
			auto &scalar = macro_func->Cast<ScalarMacroFunction>();
			ReplacePositionalParams(scalar.expression, param_name_list);
		} else {
			auto &table = macro_func->Cast<TableMacroFunction>();
			if (table.query_node) {
				ReplacePositionalParamsInQuery(*table.query_node, param_name_list);
			}
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
		bool has_language = stmt.has_language || function_def.has_language;
		auto &macro = macros.emplace_back(TransformMacroFunction(function_def, has_language));

		// Detect scalar RETURNS vs RETURNS TABLE.
		// Scalar RETURNS comes through as a single unnamed PGColumnDef in returns_table_columns.
		bool is_scalar_returns = false;
		bool is_void_returns = false;
		if (function_def.returns_table_columns && function_def.returns_table_columns->length == 1) {
			auto &first_col = PGCast<duckdb_libpgquery::PGColumnDef>(
			    *static_cast<duckdb_libpgquery::PGNode *>(function_def.returns_table_columns->head->data.ptr_value));
			if (!first_col.colname) {
				// Check for RETURNS VOID — skip type validation entirely
				if (first_col.typeName && first_col.typeName->names && first_col.typeName->names->length == 1) {
					auto name_val =
					    PGPointerCast<duckdb_libpgquery::PGValue>(first_col.typeName->names->head->data.ptr_value);
					if (name_val && name_val->val.str && strcasecmp(name_val->val.str, "void") == 0) {
						is_void_returns = true;
					}
				}
				if (!is_void_returns) {
					is_scalar_returns = true;
				}
			}
		}

		// Populate declared return types for binder validation.
		if (function_def.returns_table_columns && !is_scalar_returns && !is_void_returns) {
			// RETURNS TABLE(col type, ...)
			for (auto cell = function_def.returns_table_columns->head; cell; cell = cell->next) {
				auto &col_def = PGCast<duckdb_libpgquery::PGColumnDef>(
				    *static_cast<duckdb_libpgquery::PGNode *>(cell->data.ptr_value));
				if (col_def.typeName) {
					macro->return_types.push_back(TransformTypeName(*col_def.typeName));
				} else {
					macro->return_types.push_back(LogicalType::ANY);
				}
			}
		} else if (is_scalar_returns) {
			auto &col_def = PGCast<duckdb_libpgquery::PGColumnDef>(
			    *static_cast<duckdb_libpgquery::PGNode *>(function_def.returns_table_columns->head->data.ptr_value));
			macro->return_types.push_back(TransformTypeName(*col_def.typeName));
		} else if (function_def.returns_type) {
			// Scalar RETURNS <type> (legacy path)
			macro->return_types.push_back(
			    TransformTypeName(PGCast<duckdb_libpgquery::PGTypeName>(*function_def.returns_type)));
		}

		// RETURNS VOID: wrap body as SELECT NULL FROM (<body>) LIMIT 1.
		// The body still executes (for side effects) but result is discarded.
		if (is_void_returns && macro && macro->type == MacroType::TABLE_MACRO) {
			auto &table_macro = macro->Cast<TableMacroFunction>();
			if (table_macro.query_node) {
				auto inner_stmt = make_uniq<SelectStatement>();
				inner_stmt->node = std::move(table_macro.query_node);
				auto subquery_ref = make_uniq<SubqueryRef>(std::move(inner_stmt), "__void_body");

				auto outer = make_uniq<SelectNode>();
				outer->select_list.push_back(make_uniq<ConstantExpression>(Value()));
				outer->select_list[0]->alias = TransformQualifiedName(*stmt.name).name;
				outer->from_table = std::move(subquery_ref);

				auto limit_mod = make_uniq<LimitModifier>();
				limit_mod->limit = make_uniq<ConstantExpression>(Value::BIGINT(1));
				outer->modifiers.push_back(std::move(limit_mod));

				table_macro.query_node = std::move(outer);
			}
		}

		// For scalar RETURNS (not RETURNS TABLE), alias the single output column
		// to the function name (PG convention)
		if ((!function_def.returns_table_columns || is_scalar_returns) && macro &&
		    macro->type == MacroType::TABLE_MACRO) {
			auto &table_macro = macro->Cast<TableMacroFunction>();
			if (table_macro.query_node && table_macro.query_node->type == QueryNodeType::SELECT_NODE) {
				auto &select = table_macro.query_node->Cast<SelectNode>();
				if (select.select_list.size() == 1 && select.select_list[0]->alias.empty()) {
					select.select_list[0]->alias = TransformQualifiedName(*stmt.name).name;
				}
			}
			// PG semantics: RETURNS <scalar_type> (not TABLE / SETOF) means the
			// function produces exactly one row. Force LIMIT 1 on the query body
			// so `SELECT * FROM f()` returns at most one row, even if the body
			// query produces multiple rows (e.g. VALUES, generate_series, ...).
			// If the body already has a LIMIT, replace it with 1 (a scalar
			// function can't return more than one row regardless of what the
			// body says).
			if (table_macro.query_node) {
				auto &modifiers = table_macro.query_node->modifiers;
				// Remove any existing LIMIT (replace with 1).
				std::erase_if(modifiers, [](const unique_ptr<ResultModifier> &mod) {
					return mod->type == ResultModifierType::LIMIT_MODIFIER ||
					       mod->type == ResultModifierType::LIMIT_PERCENT_MODIFIER;
				});
				auto limit_mod = make_uniq<LimitModifier>();
				limit_mod->limit = make_uniq<ConstantExpression>(Value::BIGINT(1));
				modifiers.push_back(std::move(limit_mod));
			}
		}
	}
	PivotEntryCheck(stmt.has_language ? "function" : "macro");

	// Mark each overload as procedure
	if (stmt.is_procedure) {
		for (auto &m : macros) {
			m->is_procedure = true;
		}
	}

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
	info->is_procedure = stmt.is_procedure;
	info->macros = std::move(macros);

	result->info = std::move(info);

	return result;
}

} // namespace duckdb
