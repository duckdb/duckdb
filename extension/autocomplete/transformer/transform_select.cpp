#include "ast/join_prefix.hpp"
#include "ast/join_qualifier.hpp"
#include "ast/table_alias.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFunctionArgument(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (choice_pr->name == "NamedParameter") {
		auto parameter = transformer.Transform<MacroParameter>(choice_pr);
		parameter.expression->alias = parameter.name;
		return std::move(parameter.expression);
	}
	return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr);
}

MacroParameter PEGTransformerFactory::TransformNamedParameter(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	MacroParameter parameter;
	parameter.expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	parameter.name = list_pr.Child<IdentifierParseResult>(0).identifier;
	parameter.is_default = true;
	transformer.TransformOptional<LogicalType>(list_pr, 1, parameter.type);
	return parameter;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformTableFunctionArguments(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	// TableFunctionArguments <- Parens(List(FunctionArgument)?)
	vector<unique_ptr<ParsedExpression>> result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto stripped_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<OptionalParseResult>();
	if (stripped_parens.HasResult()) {
		auto argument_list = ExtractParseResultsFromList(stripped_parens.optional_result);
		for (auto &argument : argument_list) {
			result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(argument));
		}
	}
	return result;
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformBaseTableName(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		auto table_name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
		const auto description = TableDescription(INVALID_CATALOG, INVALID_SCHEMA, table_name);
		return make_uniq<BaseTableRef>(description);
	}
	return transformer.Transform<unique_ptr<BaseTableRef>>(choice_pr.result);
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformSchemaReservedTable(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	// SchemaReservedTable <- SchemaQualification ReservedTableName
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto table_name = list_pr.Child<IdentifierParseResult>(1).identifier;

	const auto description = TableDescription(INVALID_CATALOG, schema, table_name);
	return make_uniq<BaseTableRef>(description);
}

unique_ptr<BaseTableRef>
PEGTransformerFactory::TransformCatalogReservedSchemaTable(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	// CatalogReservedSchemaTable <- CatalogQualification ReservedSchemaQualification ReservedTableName
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto catalog = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
	auto table_name = list_pr.Child<IdentifierParseResult>(2).identifier;
	const auto description = TableDescription(catalog, schema, table_name);
	return make_uniq<BaseTableRef>(description);
}

string PEGTransformerFactory::TransformSchemaQualification(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

string PEGTransformerFactory::TransformCatalogQualification(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

QualifiedName PEGTransformerFactory::TransformQualifiedName(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<QualifiedName>(list_pr.Child<ChoiceParseResult>(0).result);
}

QualifiedName
PEGTransformerFactory::TransformCatalogReservedSchemaIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	QualifiedName result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.catalog = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return result;
}

QualifiedName PEGTransformerFactory::TransformCatalogReservedSchemaIdentifier(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	QualifiedName result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.catalog = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return result;
}

QualifiedName
PEGTransformerFactory::TransformSchemaReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	QualifiedName result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.catalog = INVALID_CATALOG;
	result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
	return result;
}

string PEGTransformerFactory::TransformReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		return choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(choice_pr.result);
}

QualifiedName
PEGTransformerFactory::TransformTableNameIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	QualifiedName result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.catalog = INVALID_CATALOG;
	result.schema = INVALID_SCHEMA;
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformWhereClause(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformTargetList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto target_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> result;
	for (auto target : target_list) {
		result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(target));
	}
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformAliasedExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformExpressionAsCollabel(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto collabel_or_string = list_pr.Child<ListParseResult>(2);
	expr->alias = transformer.Transform<string>(collabel_or_string);
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColIdExpression(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid = list_pr.Child<ListParseResult>(0);
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	expr->alias = transformer.Transform<string>(colid);
	return expr;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformExpressionOptIdentifier(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto opt_identifier = list_pr.Child<OptionalParseResult>(1);
	if (opt_identifier.HasResult()) {
		expr->alias = opt_identifier.optional_result->Cast<IdentifierParseResult>().identifier;
	}
	return expr;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableRef(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto inner_table_ref = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(0));
	auto join_or_pivot_opt = list_pr.Child<OptionalParseResult>(1);
	if (join_or_pivot_opt.HasResult()) {
		auto repeat_join_or_pivot = join_or_pivot_opt.optional_result->Cast<RepeatParseResult>();
		for (auto join_or_pivot : repeat_join_or_pivot.children) {
			auto transform_join_or_pivot = transformer.Transform<unique_ptr<TableRef>>(join_or_pivot);
			if (transform_join_or_pivot->type == TableReferenceType::JOIN) {
				auto join_ref = unique_ptr_cast<TableRef, JoinRef>(std::move(transform_join_or_pivot));
				join_ref->left = std::move(inner_table_ref);
				inner_table_ref = std::move(join_ref);
			}
		}
	}

	auto opt_table_alias = list_pr.Child<OptionalParseResult>(2);
	if (opt_table_alias.HasResult()) {
		auto table_alias = transformer.Transform<TableAlias>(opt_table_alias.optional_result);
		inner_table_ref->alias = table_alias.name;
		inner_table_ref->column_name_alias = table_alias.column_name_alias;
	}
	return inner_table_ref;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformJoinOrPivot(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformJoinClause(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformRegularJoinClause(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<JoinRef>();
	auto asof = list_pr.Child<OptionalParseResult>(0);
	if (asof.HasResult()) {
		throw NotImplementedException("ASOF join not implemented");
	}
	auto join_type = JoinType::INNER;
	transformer.TransformOptional<JoinType>(list_pr, 1, join_type);
	result->type = join_type;
	result->right = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(3));
	auto join_qualifier = transformer.Transform<JoinQualifier>(list_pr.Child<ListParseResult>(4));
	if (join_qualifier.on_clause) {
		result->condition = std::move(join_qualifier.on_clause);
	} else if (!join_qualifier.using_columns.empty()) {
		result->using_columns = std::move(join_qualifier.using_columns);
	} else {
		throw InternalException("Invalid join qualifier found.");
	}
	return std::move(result);
}

JoinType PEGTransformerFactory::TransformJoinType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<JoinType>(list_pr.Child<ChoiceParseResult>(0).result);
}

JoinQualifier PEGTransformerFactory::TransformJoinQualifier(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<JoinQualifier>(list_pr.Child<ChoiceParseResult>(0).result);
}

JoinQualifier PEGTransformerFactory::TransformOnClause(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	JoinQualifier result;
	result.on_clause = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	return result;
}

JoinQualifier PEGTransformerFactory::TransformUsingClause(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	JoinQualifier result;
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto column_list = ExtractParseResultsFromList(extract_parens);
	for (auto column : column_list) {
		result.using_columns.push_back(column->Cast<IdentifierParseResult>().identifier);
	}
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformJoinWithoutOnClause(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto join_prefix = transformer.Transform<JoinPrefix>(list_pr.Child<ListParseResult>(0));
	auto table_ref = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(2));
	auto result = make_uniq<JoinRef>();
	result->ref_type = join_prefix.ref_type;
	result->type = join_prefix.join_type;
	result->right = std::move(table_ref);
	return std::move(result);
}

JoinPrefix PEGTransformerFactory::TransformJoinPrefix(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<JoinPrefix>(list_pr.Child<ChoiceParseResult>(0).result);
}

JoinPrefix PEGTransformerFactory::TransformCrossJoinPrefix(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	JoinPrefix result;
	result.ref_type = JoinRefType::CROSS;
	result.join_type = JoinType::INNER;
	return result;
}

JoinPrefix PEGTransformerFactory::TransformNaturalJoinPrefix(PEGTransformer &transformer,
                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	JoinPrefix result;
	result.ref_type = JoinRefType::NATURAL;
	transformer.TransformOptional<JoinType>(list_pr, 1, result.join_type);
	return result;
}

JoinPrefix PEGTransformerFactory::TransformPositionalJoinPrefix(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	JoinPrefix result;
	result.ref_type = JoinRefType::POSITIONAL;
	result.join_type = JoinType::INNER;
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformInnerTableRef(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableFunction(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableFunctionLateralOpt(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto result = make_uniq<TableFunctionRef>();

	// TODO(Dtenwolde) Figure out what to do with lateral
	if (list_pr.Child<OptionalParseResult>(0).HasResult()) {
		throw NotImplementedException("Lateral has not yet been implemented");
	};
	auto qualified_table_function = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(1));
	auto table_function_arguments =
	    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(2));

	result->function =
	    make_uniq<FunctionExpression>(qualified_table_function.catalog, qualified_table_function.schema,
	                                  qualified_table_function.name, std::move(table_function_arguments));
	auto table_alias_opt = list_pr.Child<OptionalParseResult>(3);
	if (table_alias_opt.HasResult()) {
		auto table_alias = transformer.Transform<TableAlias>(table_alias_opt.optional_result);
		result->alias = table_alias.name;
		result->column_name_alias = table_alias.column_name_alias;
	}
	return std::move(result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableFunctionAliasColon(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto table_alias = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));

	auto qualified_table_function = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(1));
	auto table_function_arguments =
	    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(2));

	auto result = make_uniq<TableFunctionRef>();
	result->function =
	    make_uniq<FunctionExpression>(qualified_table_function.catalog, qualified_table_function.schema,
	                                  qualified_table_function.name, std::move(table_function_arguments));
	result->alias = table_alias;
	return std::move(result);
}

string PEGTransformerFactory::TransformTableAliasColon(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
}

QualifiedName PEGTransformerFactory::TransformQualifiedTableFunction(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	auto opt_catalog = list_pr.Child<OptionalParseResult>(0);
	if (opt_catalog.HasResult()) {
		result.catalog = transformer.Transform<string>(opt_catalog.optional_result);
	} else {
		result.catalog = INVALID_CATALOG;
	}
	auto opt_schema = list_pr.Child<OptionalParseResult>(1);
	if (opt_schema.HasResult()) {
		result.schema = transformer.Transform<string>(opt_schema.optional_result);
	} else {
		result.schema = INVALID_SCHEMA;
	}
	result.name = list_pr.Child<IdentifierParseResult>(2).identifier;
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableSubquery(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	// TODO(Dtenwolde)
	if (list_pr.Child<OptionalParseResult>(0).HasResult()) {
		throw NotImplementedException("LATERAL has not yet been implemented");
	};
	auto subquery_reference = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(1));
	auto table_alias_opt = list_pr.Child<OptionalParseResult>(2);
	if (table_alias_opt.HasResult()) {
		auto table_alias = transformer.Transform<TableAlias>(table_alias_opt.optional_result);
		subquery_reference->alias = table_alias.name;
		subquery_reference->column_name_alias = table_alias.column_name_alias;
	}
	return subquery_reference;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformSubqueryReference(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(select_statement));
	return std::move(subquery_ref);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformBaseTableRef(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(1));
	auto table_alias_colon_opt = list_pr.Child<OptionalParseResult>(0);
	if (table_alias_colon_opt.HasResult()) {
		result->alias = transformer.Transform<string>(table_alias_colon_opt.optional_result);
	}
	auto table_alias_opt = list_pr.Child<OptionalParseResult>(2);
	if (table_alias_opt.HasResult() && table_alias_colon_opt.HasResult()) {
		throw ParserException("Table reference %s cannot have two aliases", result->ToString());
	}
	if (table_alias_opt.HasResult()) {
		auto table_alias = transformer.Transform<TableAlias>(table_alias_opt.optional_result);
		result->alias = table_alias.name;
		result->column_name_alias = table_alias.column_name_alias;
	}
	transformer.TransformOptional<unique_ptr<AtClause>>(list_pr, 3, result->at_clause);
	return std::move(result);
}

unique_ptr<AtClause> PEGTransformerFactory::TransformAtClause(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	return transformer.Transform<unique_ptr<AtClause>>(extract_parens);
}

unique_ptr<AtClause> PEGTransformerFactory::TransformAtSpecifier(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto unit = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	return make_uniq<AtClause>(unit, std::move(expr));
}

string PEGTransformerFactory::TransformAtUnit(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return choice_pr.result->Cast<KeywordParseResult>().keyword;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformValuesRef(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto values_select_statement =
	    transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(0));
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(values_select_statement));
	auto opt_alias = list_pr.Child<OptionalParseResult>(1);
	if (opt_alias.HasResult()) {
		auto table_alias = transformer.Transform<TableAlias>(opt_alias.optional_result);
		subquery_ref->alias = table_alias.name;
		subquery_ref->column_name_alias = table_alias.column_name_alias;
	}
	return std::move(subquery_ref);
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformValuesClause(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto value_expression_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(1));
	vector<vector<unique_ptr<ParsedExpression>>> values_list;
	for (auto value_expression : value_expression_list) {
		values_list.push_back(transformer.Transform<vector<unique_ptr<ParsedExpression>>>(value_expression));
	}
	if (values_list.size() > 1) {
		const auto expected_size = values_list[0].size();
		for (idx_t i = 1; i < values_list.size(); i++) {
			if (values_list[i].size() != expected_size) {
				throw ParserException("VALUES lists must all be the same length");
			}
		}
	}
	auto result = make_uniq<ExpressionListRef>();
	result->alias = "valueslist";
	result->values = std::move(values_list);

	auto select_node = make_uniq<SelectNode>();
	select_node->from_table = std::move(result);
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return select_statement;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformValuesExpressions(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> result;
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto expression_list = ExtractParseResultsFromList(extract_parens);
	for (auto expression : expression_list) {
		result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expression));
	}
	return result;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformTableStatement(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<SelectStatement>();
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(1));
	result->node = std::move(node);
	return result;
}

vector<OrderByNode> PEGTransformerFactory::TransformOrderByClause(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<OrderByNode>>(list_pr.Child<ListParseResult>(2));
}

vector<OrderByNode> PEGTransformerFactory::TransformOrderByExpressions(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<OrderByNode>>(list_pr.Child<ChoiceParseResult>(0).result);
}

vector<OrderByNode> PEGTransformerFactory::TransformOrderByExpressionList(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<OrderByNode> result;
	auto expr_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	for (auto expr : expr_list) {
		result.push_back(transformer.Transform<OrderByNode>(expr));
	}
	return result;
}

vector<OrderByNode> PEGTransformerFactory::TransformOrderByAll(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<OrderByNode> result;
	auto order_type = OrderType::ORDER_DEFAULT;
	auto order_type_pr = list_pr.Child<OptionalParseResult>(1);
	if (order_type_pr.HasResult()) {
		order_type = transformer.Transform<OrderType>(order_type_pr.optional_result);
	}
	auto order_by_null_type = OrderByNullType::ORDER_DEFAULT;
	auto order_by_null_pr = list_pr.Child<OptionalParseResult>(2);
	if (order_by_null_pr.HasResult()) {
		order_by_null_type = transformer.Transform<OrderByNullType>(order_by_null_pr.optional_result);
	}
	auto star_expr = make_uniq<StarExpression>();
	star_expr->columns = true;
	result.push_back(OrderByNode(order_type, order_by_null_type, std::move(star_expr)));
	return result;
}

OrderByNode PEGTransformerFactory::TransformOrderByExpression(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto order_type = OrderType::ORDER_DEFAULT;
	transformer.TransformOptional<OrderType>(list_pr, 1, order_type);
	auto order_by_null_type = OrderByNullType::ORDER_DEFAULT;
	transformer.TransformOptional<OrderByNullType>(list_pr, 2, order_by_null_type);
	return OrderByNode(order_type, order_by_null_type, std::move(expr));
}

OrderType PEGTransformerFactory::TransformDescOrAsc(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<OrderType>(list_pr.Child<ChoiceParseResult>(0).result);
}

OrderByNullType PEGTransformerFactory::TransformNullsFirstOrLast(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<OrderByNullType>(list_pr.Child<ChoiceParseResult>(0).result);
}

} // namespace duckdb
