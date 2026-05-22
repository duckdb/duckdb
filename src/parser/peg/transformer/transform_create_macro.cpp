#include "duckdb/parser/peg/ast/macro_parameter.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/result_modifier.hpp"

#include <charconv>

namespace duckdb {

// Replaces positional ParameterExpression nodes ($1, $2, ...) in a macro body
// with ColumnRefExpression nodes referring to the macro's positional parameter
// names. Without this rewrite, the binder sees `$N` as a prepared-statement
// parameter and fails to mark macros as `bound_all_parameters`.
static void ReplacePositionalParams(unique_ptr<ParsedExpression> &expr, const vector<string> &param_names) {
	if (!expr) {
		return;
	}
	if (expr->GetExpressionClass() == ExpressionClass::PARAMETER) {
		auto &param = expr->Cast<ParameterExpression>();
		idx_t idx = 0;
		auto [ptr, ec] =
		    std::from_chars(param.identifier.data(), param.identifier.data() + param.identifier.size(), idx);
		if (ec == std::errc() && ptr == param.identifier.data() + param.identifier.size() && idx >= 1 &&
		    idx <= param_names.size()) {
			auto replacement = make_uniq<ColumnRefExpression>(param_names[idx - 1]);
			replacement->SetAlias(expr->GetAlias());
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

static void ApplyPositionalParamRewrite(MacroFunction &macro) {
	if (macro.parameters.empty()) {
		return;
	}
	vector<string> param_name_list;
	for (auto &p : macro.parameters) {
		if (p->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			param_name_list.push_back(p->Cast<ColumnRefExpression>().GetColumnName());
		} else {
			// Non-column-ref parameter (unlikely after our normalization); skip.
			return;
		}
	}
	if (macro.type == MacroType::SCALAR_MACRO) {
		auto &scalar = macro.Cast<ScalarMacroFunction>();
		ReplacePositionalParams(scalar.expression, param_name_list);
	} else {
		auto &table = macro.Cast<TableMacroFunction>();
		if (table.query_node) {
			ReplacePositionalParamsInQuery(*table.query_node, param_name_list);
		}
	}
}
// Read the MacroOrFunction keyword choice (MACRO / FUNCTION / PROCEDURE) and
// return true iff PROCEDURE was used. The procedure flag changes binder
// behavior (procedures must be invoked via CALL, not implicit table function).
static bool IsProcedureKeyword(ParseResult &macro_or_function_pr) {
	auto &list_pr = macro_or_function_pr.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return StringUtil::CIEquals(choice_pr.Cast<KeywordParseResult>().keyword, "procedure");
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateMacroStmt(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateMacroInfo>(CatalogType::MACRO_ENTRY);

	auto is_procedure = IsProcedureKeyword(list_pr.Child<ListParseResult>(0));
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto qualified_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2));
	if (qualified_name.schema.empty()) {
		info->schema = qualified_name.catalog;
	} else {
		info->catalog = qualified_name.catalog;
		info->schema = qualified_name.schema;
	}
	info->name = qualified_name.name;
	auto macro_definition_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(3));

	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->is_procedure = is_procedure;
	vector<unique_ptr<MacroFunction>> macro_functions;
	for (auto macro_definition : macro_definition_list) {
		auto &macro = info->macros.emplace_back(transformer.Transform<unique_ptr<MacroFunction>>(macro_definition));
		if (is_procedure) {
			macro->is_procedure = true;
		}
	}
	D_ASSERT(!info->macros.empty());
	auto macro_type = info->macros[0]->type;
	if (info->macros.size() > 1) {
		for (idx_t i = 1; i < macro_definition_list.size(); ++i) {
			if (info->macros[i]->type != macro_type) {
				throw ParserException("Cannot mix table and scalar macro function definitions");
			}
		}
	}
	info->type = macro_type == MacroType::TABLE_MACRO ? CatalogType::TABLE_MACRO_ENTRY : CatalogType::MACRO_ENTRY;
	result->info = std::move(info);
	transformer.PivotEntryCheck("macro");
	return result;
}

struct FunctionDecoratorInfo {
	bool has_language = false;
};

// Process the FunctionDecorator* repeat block of a MacroDefinition and apply
// it to the macro function. The decorators are PG-compat extras (RETURNS, ...).
// Most semantics are dropped, but RETURNS TABLE/Type populates
// return_types/return_names which the catalog uses to expose schema.
static void ApplyFunctionDecorators(PEGTransformer &transformer, RepeatParseResult &decorators_pr,
                                    MacroFunction &macro_function, FunctionDecoratorInfo &info) {
	auto children = decorators_pr.GetChildren();
	for (auto &child_ref : children) {
		auto &decorator_list = child_ref.get().Cast<ListParseResult>();
		// FunctionDecorator <- ReturnsClause / VolatilityKeyword / StrictClause / LanguageClause / SecurityClause /
		// CostClause / RowsClause / ParallelClause / LeakproofClause
		auto &decorator_choice = decorator_list.Child<ChoiceParseResult>(0).GetResult();
		if (decorator_choice.name == "ReturnsClause") {
			// ReturnsClause <- 'RETURNS' (ReturnsTable / ReturnsNull / Type)
			auto &returns_list = decorator_choice.Cast<ListParseResult>();
			auto &inner_group = returns_list.Child<ListParseResult>(1);
			auto &returns_choice = inner_group.Child<ChoiceParseResult>(0).GetResult();
			if (returns_choice.name == "ReturnsTable") {
				// ReturnsTable <- 'TABLE' Parens(List(ColIdType))
				auto &ret_table_list = returns_choice.Cast<ListParseResult>();
				auto &inner_list =
				    PEGTransformerFactory::ExtractResultFromParens(ret_table_list.Child<ListParseResult>(1));
				auto colid_type_list = PEGTransformerFactory::ExtractParseResultsFromList(inner_list);
				for (auto colid_type : colid_type_list) {
					auto pair = transformer.Transform<std::pair<string, LogicalType>>(colid_type);
					macro_function.return_names.push_back(pair.first);
					macro_function.return_types.push_back(std::move(pair.second));
				}
			} else if (returns_choice.name == "ReturnsNull") {
				// RETURNS NULL ON NULL INPUT -- a strictness marker, not a return type.
				// No-op for now.
			} else {
				// RETURNS <type>
				macro_function.return_types.push_back(transformer.Transform<LogicalType>(returns_choice));
			}
		} else if (decorator_choice.name == "LanguageClause") {
			info.has_language = true;
		}
		// Other decorators (volatility, strict, security, cost, rows, parallel, leakproof)
		// are accepted for PG-syntax compat but their semantics are dropped.
	}
}

unique_ptr<MacroFunction> PEGTransformerFactory::TransformMacroDefinition(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();

	// MacroDefinition <- Parens(MacroParameters?) FunctionDecorator* (AsMacroBody / AtomicMacroBody / ReturnMacroBody)
	// children: 0 = Parens(MacroParameters?), 1 = FunctionDecorator* (Repeat), 2 = body choice group.
	auto &body_pr = list_pr.Child<ListParseResult>(2);
	auto macro_function =
	    transformer.Transform<unique_ptr<MacroFunction>>(body_pr.Child<ChoiceParseResult>(0).GetResult());
	auto &parameters_pr = ExtractResultFromParens(list_pr.Child<ListParseResult>(0)).Cast<OptionalParseResult>();
	// Collect parameters up front but defer applying them; bare `TYPE` params
	// need to know whether LANGUAGE was specified to pick PG vs DuckDB semantics.
	vector<MacroParameter> parameters;
	if (parameters_pr.HasResult()) {
		parameters = transformer.Transform<vector<MacroParameter>>(parameters_pr.GetResult());
	}
	// FunctionDecorator* is wrapped in an OptionalParseResult(RepeatParseResult).
	// We allow decorators both BEFORE and AFTER the body to accept both PG-style
	// orderings (`LANGUAGE SQL AS ...` vs `AS $$..$$ LANGUAGE sql`).
	FunctionDecoratorInfo decorator_info;
	auto apply_decorators_at = [&](idx_t pos) {
		auto &decorators_opt = list_pr.Child<OptionalParseResult>(pos);
		if (decorators_opt.HasResult()) {
			auto &decorators_pr = decorators_opt.GetResult().Cast<RepeatParseResult>();
			ApplyFunctionDecorators(transformer, decorators_pr, *macro_function, decorator_info);
		}
	};
	apply_decorators_at(1);
	apply_decorators_at(3);

	// Now apply the params, knowing has_language. Mirrors the v2026.05.18
	// `param.ambiguous` branch in TransformMacroFunction:
	//  - has_language: bare TYPE -> positional, name $N. Resolve the type via
	//    DefaultTypeGenerator when it's a built-in name (bigint, int, text,
	//    ...). User-defined names (pg_attribute, ...) get UNKNOWN so the
	//    binder doesn't require a coercion that has no cast registered.
	//  - !has_language: bare TYPE -> DuckDB named-param, drop the type.
	if (!parameters.empty()) {
		bool default_value_found = false;
		case_insensitive_string_set_t parameter_names;
		for (auto &parameter : parameters) {
			if (parameter.ambiguous_bare_type) {
				if (decorator_info.has_language) {
					auto pos_name = "$" + std::to_string(macro_function->parameters.size() + 1);
					auto builtin = DefaultTypeGenerator::GetDefaultType(parameter.name);
					parameter.type = builtin.id() != LogicalTypeId::INVALID ? builtin : LogicalType::UNKNOWN;
					parameter.name = pos_name;
					parameter.expression = make_uniq<ColumnRefExpression>(pos_name);
				} else {
					parameter.type = LogicalType::UNKNOWN;
				}
			}
			D_ASSERT(!parameter.name.empty());
			if (parameter_names.find(parameter.name) != parameter_names.end()) {
				throw ParserException("Duplicate parameter '%s' in macro definition", parameter.name);
			}
			parameter_names.insert(parameter.name);
			if (parameter.is_default) {
				auto default_expr = std::move(parameter.expression);
				default_expr->SetAlias(parameter.name);
				macro_function->default_parameters[parameter.name] = std::move(default_expr);
				macro_function->parameters.push_back(make_uniq<ColumnRefExpression>(parameter.name));
				default_value_found = true;
			} else {
				if (default_value_found) {
					throw ParserException("Parameter without a default follows parameter with a default");
				}
				macro_function->parameters.push_back(std::move(parameter.expression));
			}
			macro_function->types.push_back(parameter.type);
		}
	}

	// PG-style SQL body: when LANGUAGE was specified AND the body is a string
	// constant scalar macro (parsed via `AS 'SELECT ...'`), re-parse the string
	// as SQL and replace the scalar body with the parsed query node.
	if (decorator_info.has_language && macro_function->type == MacroType::SCALAR_MACRO) {
		auto &scalar = macro_function->Cast<ScalarMacroFunction>();
		if (scalar.expression && scalar.expression->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &const_expr = scalar.expression->Cast<ConstantExpression>();
			auto val = const_expr.GetValue();
			if (val.type().id() == LogicalTypeId::VARCHAR && !val.IsNull()) {
				auto body_string = val.GetValue<string>();
				Parser parser;
				parser.ParseQuery(body_string);
				if (parser.statements.size() != 1) {
					throw ParserException("Function body must contain exactly one statement");
				}
				auto &stmt = *parser.statements[0];
				// PG-compat: the SQL body may be SELECT (read-only) or DML
				// (INSERT/UPDATE/DELETE). All of these store a QueryNode subclass
				// that TableMacroFunction can wrap, so procedures with DML bodies
				// execute the statement on CALL instead of returning the body text
				// as a scalar string.
				unique_ptr<QueryNode> body_node;
				switch (stmt.type) {
				case StatementType::SELECT_STATEMENT:
					body_node = std::move(stmt.Cast<SelectStatement>().node);
					break;
				case StatementType::INSERT_STATEMENT:
					body_node = std::move(stmt.Cast<InsertStatement>().node);
					break;
				case StatementType::UPDATE_STATEMENT:
					body_node = std::move(stmt.Cast<UpdateStatement>().node);
					break;
				case StatementType::DELETE_STATEMENT:
					body_node = std::move(stmt.Cast<DeleteStatement>().node);
					break;
				default:
					throw ParserException("Unsupported statement type in SQL function/procedure body: %s", body_string);
				}
				auto table_macro = make_uniq<TableMacroFunction>();
				table_macro->query_node = std::move(body_node);
				// preserve params and return types
				table_macro->return_types = std::move(macro_function->return_types);
				table_macro->return_names = std::move(macro_function->return_names);
				table_macro->parameters = std::move(macro_function->parameters);
				table_macro->default_parameters = std::move(macro_function->default_parameters);
				table_macro->types = std::move(macro_function->types);
				table_macro->is_procedure = macro_function->is_procedure;
				macro_function = std::move(table_macro);
			}
		}
	}

	// PG-compat: when the user declared `RETURNS TABLE(col type, ...)` and
	// the body is a SELECT, propagate the declared column names onto the
	// body. For `SELECT *`-shaped bodies (e.g. a VALUES table), push the
	// names onto the FROM table's column_name_alias (and
	// ExpressionListRef::expected_names for VALUES). For per-expression
	// bodies, alias each select_list entry.
	if (!macro_function->return_names.empty() && macro_function->type == MacroType::TABLE_MACRO) {
		auto &table_macro = macro_function->Cast<TableMacroFunction>();
		if (table_macro.query_node && table_macro.query_node->type == QueryNodeType::SELECT_NODE) {
			auto &select = table_macro.query_node->Cast<SelectNode>();
			const auto &column_names = macro_function->return_names;
			const bool is_star_select =
			    select.select_list.size() == 1 && select.select_list[0]->GetExpressionType() == ExpressionType::STAR;
			if (is_star_select && select.from_table) {
				select.from_table->column_name_alias = column_names;
				if (select.from_table->type == TableReferenceType::EXPRESSION_LIST) {
					select.from_table->Cast<ExpressionListRef>().expected_names = column_names;
				}
			} else {
				for (idx_t i = 0; i < column_names.size() && i < select.select_list.size(); ++i) {
					select.select_list[i]->SetAlias(column_names[i]);
				}
			}
		}
	}

	// Replace any positional $N references in the body with named ColumnRefs
	// keyed off the parameter list. Must run AFTER any LANGUAGE-SQL rewrite
	// above (which may flip scalar -> table macro) and BEFORE the scalar-shoehorn
	// wrap below (so the wrapped SubqueryExpression sees substituted refs —
	// EnumerateChildren on SubqueryExpression doesn't recurse into the inner
	// query_node, so post-wrap traversal would miss them).
	ApplyPositionalParamRewrite(*macro_function);

	// Scalar `RETURNS <type>` with a SELECT-shaped body (BEGIN ATOMIC ... END,
	// AS TABLE SELECT, or LANGUAGE-SQL string body that became a SELECT above)
	// is a "scalar shoehorn": wrap the table macro's query as a scalar subquery
	// (LIMIT 1). 0 rows -> NULL, 1 row -> value, >1 rows is bounded by the LIMIT.
	const bool is_scalar_returns = !macro_function->return_types.empty() && macro_function->return_names.empty();
	if (is_scalar_returns && macro_function->type == MacroType::TABLE_MACRO) {
		auto &table_macro = macro_function->Cast<TableMacroFunction>();
		if (table_macro.query_node) {
			auto &modifiers = table_macro.query_node->modifiers;
			std::erase_if(modifiers, [](const unique_ptr<ResultModifier> &mod) {
				return mod->type == ResultModifierType::LIMIT_MODIFIER ||
				       mod->type == ResultModifierType::LIMIT_PERCENT_MODIFIER;
			});
			auto limit_mod = make_uniq<LimitModifier>();
			limit_mod->limit = make_uniq<ConstantExpression>(Value::BIGINT(1));
			modifiers.push_back(std::move(limit_mod));

			auto inner_stmt = make_uniq<SelectStatement>();
			inner_stmt->node = std::move(table_macro.query_node);
			auto subquery = make_uniq<SubqueryExpression>();
			subquery->subquery = std::move(inner_stmt);
			subquery->subquery_type = SubqueryType::SCALAR;

			auto scalar_macro = make_uniq<ScalarMacroFunction>(std::move(subquery));
			scalar_macro->parameters = std::move(table_macro.parameters);
			scalar_macro->types = std::move(table_macro.types);
			scalar_macro->default_parameters = std::move(table_macro.default_parameters);
			scalar_macro->return_types = std::move(table_macro.return_types);
			scalar_macro->return_names = std::move(table_macro.return_names);
			scalar_macro->is_procedure = table_macro.is_procedure;
			macro_function = std::move(scalar_macro);
		}
	}

	return macro_function;
}

unique_ptr<MacroFunction> PEGTransformerFactory::TransformTableMacroDefinition(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<TableMacroFunction>();
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));
	result->query_node = std::move(select_statement->node);
	return std::move(result);
}

// AsMacroBody <- 'AS' (TableMacroDefinition / ScalarMacroDefinition)
unique_ptr<MacroFunction> PEGTransformerFactory::TransformAsMacroBody(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &group = list_pr.Child<ListParseResult>(1);
	return transformer.Transform<unique_ptr<MacroFunction>>(group.Child<ChoiceParseResult>(0).GetResult());
}

// AtomicMacroBody <- 'BEGIN' 'ATOMIC' SelectStatementInternal ';'? 'END'
// A `BEGIN ATOMIC ... END` body always produces a TableMacroFunction (the body is a SELECT).
unique_ptr<MacroFunction> PEGTransformerFactory::TransformAtomicMacroBody(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<TableMacroFunction>();
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(2));
	result->query_node = std::move(select_statement->node);
	return std::move(result);
}

// ReturnMacroBody <- 'RETURN' Expression
unique_ptr<MacroFunction> PEGTransformerFactory::TransformReturnMacroBody(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<ScalarMacroFunction>();
	result->expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	return std::move(result);
}

unique_ptr<MacroFunction> PEGTransformerFactory::TransformScalarMacroDefinition(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<ScalarMacroFunction>();
	result->expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	return std::move(result);
}

vector<MacroParameter> PEGTransformerFactory::TransformMacroParameters(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto parameter_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<MacroParameter> parameters;
	for (auto parameter : parameter_list) {
		parameters.push_back(transformer.Transform<MacroParameter>(parameter));
	}
	return parameters;
}

MacroParameter PEGTransformerFactory::TransformMacroParameter(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return transformer.Transform<MacroParameter>(choice_pr);
}

MacroParameter PEGTransformerFactory::TransformSimpleParameter(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto parameter = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	MacroParameter result;
	result.name = parameter;
	result.expression = make_uniq<ColumnRefExpression>(parameter);
	auto &opt_type = list_pr.Child<OptionalParseResult>(1);
	if (opt_type.HasResult()) {
		result.type = transformer.Transform<LogicalType>(opt_type.GetResult());
	} else {
		// Bare token with no Type after it: ambiguous between DuckDB
		// `name` and PG positional `TYPE`. Defer the decision until we know
		// whether LANGUAGE was specified (set in TransformMacroDefinition).
		result.ambiguous_bare_type = true;
	}
	result.is_default = false;
	return result;
}

} // namespace duckdb
