#include "ast/join_prefix.hpp"
#include "ast/join_qualifier.hpp"
#include "ast/limit_percent_result.hpp"
#include "ast/table_alias.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformSelectStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	throw NotImplementedException("TransformSelectStatement");
	return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<SelectStatement>
PEGTransformerFactory::TransformSelectStatementInternal(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ResultModifier>> result_modifiers;
	transformer.TransformOptional<vector<unique_ptr<ResultModifier>>>(list_pr, 2, result_modifiers);
	for (auto &result_modifier : result_modifiers) {
		select_statement->node->modifiers.push_back(std::move(result_modifier));
	}
	return select_statement;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSelectOrParens(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSelectParens(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformBaseSelect(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));
	transformer.TransformOptional<vector<unique_ptr<ResultModifier>>>(list_pr, 2, select_statement->node->modifiers);
	auto with_clause = list_pr.Child<OptionalParseResult>(0);
	if (with_clause.HasResult()) {
		select_statement->node->cte_map = transformer.Transform<CommonTableExpressionMap>(with_clause.optional_result);
	}

	return select_statement;
}

unique_ptr<SelectStatement>
PEGTransformerFactory::TransformSelectStatementType(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SelectStatement>
PEGTransformerFactory::TransformOptionalParensSimpleSelect(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSimpleSelectParens(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSimpleSelect(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select_from = transformer.Transform<unique_ptr<SelectNode>>(list_pr.Child<ListParseResult>(0));
	auto opt_where_clause = list_pr.Child<OptionalParseResult>(1);
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 1, select_from->where_clause);
	transformer.TransformOptional<GroupByNode>(list_pr, 2, select_from->groups);
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, select_from->having);
	auto opt_window_clause = list_pr.Child<OptionalParseResult>(4);
	if (opt_window_clause.HasResult()) {
		throw NotImplementedException("Window clause in SELECT statement has not yet been implemented.");
	}
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 5, select_from->qualify);
	transformer.TransformOptional<unique_ptr<SampleOptions>>(list_pr, 6, select_from->sample);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_from);
	return select_statement;
}

unique_ptr<SelectNode> PEGTransformerFactory::TransformSelectFrom(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SelectNode>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SelectNode> PEGTransformerFactory::TransformSelectFromClause(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list =
	    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(0));
	auto opt_from = list_pr.Child<OptionalParseResult>(1);
	if (opt_from.HasResult()) {
		select_node->from_table = transformer.Transform<unique_ptr<TableRef>>(opt_from.optional_result);
	} else {
		select_node->from_table = make_uniq<EmptyTableRef>();
	}
	return select_node;
}

unique_ptr<SelectNode> PEGTransformerFactory::TransformFromSelectClause(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select_node = make_uniq<SelectNode>();
	select_node->from_table = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(0));
	auto opt_select = list_pr.Child<OptionalParseResult>(1);
	if (opt_select.HasResult()) {
		select_node->select_list =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_select.optional_result);
	} else {
		select_node->select_list.push_back(make_uniq<StarExpression>());
	}
	return select_node;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformFromClause(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table_ref_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(1));
	auto result_table_ref = transformer.Transform<unique_ptr<TableRef>>(table_ref_list[0]);
	if (table_ref_list.size() == 1) {
		return result_table_ref;
	}
	for (idx_t i = 1; i < table_ref_list.size(); i++) {
		auto table_ref = transformer.Transform<unique_ptr<TableRef>>(table_ref_list[i]);
		auto cross_product = make_uniq<JoinRef>();
		cross_product->left = std::move(result_table_ref);
		cross_product->right = std::move(table_ref);
		cross_product->ref_type = JoinRefType::CROSS;
		cross_product->is_implicit = true;
		result_table_ref = std::move(cross_product);
	}
	return result_table_ref;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSelectClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	// TODO(Dtenwolde) Do something with opt_distinct
	auto opt_distinct = list_pr.Child<OptionalParseResult>(1);
	auto target_list = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(2));
	return target_list;
}

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

TableAlias PEGTransformerFactory::TransformTableAlias(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	TableAlias result;
	auto qualified_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(1));
	result.name = qualified_name.name;
	auto opt_column_aliases = list_pr.Child<OptionalParseResult>(2);
	if (opt_column_aliases.HasResult()) {
		result.column_name_alias = transformer.Transform<vector<string>>(opt_column_aliases.optional_result);
	}
	return result;
}

vector<string> PEGTransformerFactory::TransformColumnAliases(PEGTransformer &transformer,
                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> result;
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto alias_list = ExtractParseResultsFromList(extract_parens);
	for (auto alias : alias_list) {
		result.push_back(transformer.Transform<string>(alias));
	}
	return result;
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
	auto table_alias_opt = list_pr.Child<OptionalParseResult>(4);
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

vector<unique_ptr<ResultModifier>>
PEGTransformerFactory::TransformResultModifiers(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ResultModifier>> result;
	vector<OrderByNode> order_by;
	transformer.TransformOptional<vector<OrderByNode>>(list_pr, 0, order_by);
	if (!order_by.empty()) {
		auto order_modifier = make_uniq<OrderModifier>();
		order_modifier->orders = std::move(order_by);
		result.push_back(std::move(order_modifier));
	}
	unique_ptr<ResultModifier> limit_offset;
	transformer.TransformOptional<unique_ptr<ResultModifier>>(list_pr, 1, limit_offset);
	if (limit_offset) {
		result.push_back(std::move(limit_offset));
	}
	return result;
}

unique_ptr<ResultModifier> PEGTransformerFactory::TransformLimitOffsetClause(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	LimitPercentResult limit_percent;
	LimitPercentResult offset_percent;
	transformer.TransformOptional<LimitPercentResult>(list_pr, 0, limit_percent);
	transformer.TransformOptional<LimitPercentResult>(list_pr, 1, offset_percent);
	if (offset_percent.is_percent) {
		throw ParserException("Percentage for offsets are not supported.");
	}
	if (limit_percent.is_percent) {
		auto result = make_uniq<LimitPercentModifier>();
		result->limit = std::move(limit_percent.expression);
		result->offset = std::move(offset_percent.expression);
		return result;
	} else {
		auto result = make_uniq<LimitModifier>();
		if (limit_percent.expression) {
			result->limit = std::move(limit_percent.expression);
		}
		if (offset_percent.expression) {
			result->offset = std::move(offset_percent.expression);
		}
		if (!result->limit && !result->offset) {
			return nullptr;
		}
		return result;
	}
}

LimitPercentResult PEGTransformerFactory::TransformLimitClause(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<LimitPercentResult>(list_pr.Child<ListParseResult>(1));
}

LimitPercentResult PEGTransformerFactory::TransformLimitValue(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<LimitPercentResult>(list_pr.Child<ChoiceParseResult>(0).result);
}

LimitPercentResult PEGTransformerFactory::TransformLimitAll(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	LimitPercentResult result;
	result.expression = make_uniq<StarExpression>();
	result.is_percent = false;
	return result;
}

LimitPercentResult PEGTransformerFactory::TransformLimitLiteralPercent(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	LimitPercentResult result;
	// TODO(Dtenwolde) transform number literal properly
	result.expression = make_uniq<ConstantExpression>(Value(list_pr.Child<NumberParseResult>(0).number));
	result.is_percent = true;
	return result;
}

LimitPercentResult PEGTransformerFactory::TransformLimitExpression(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	LimitPercentResult result;
	result.expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	result.is_percent = list_pr.Child<OptionalParseResult>(1).HasResult();
	return result;
}

LimitPercentResult PEGTransformerFactory::TransformOffsetClause(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<LimitPercentResult>(list_pr.Child<ListParseResult>(1));
}

GroupByNode PEGTransformerFactory::TransformGroupByClause(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<GroupByNode>(list_pr.Child<ListParseResult>(2));
}

GroupByNode PEGTransformerFactory::TransformGroupByExpressions(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<GroupByNode>(list_pr.Child<ChoiceParseResult>(0).result);
}

GroupByNode PEGTransformerFactory::TransformGroupByAll(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	GroupByNode result;
	result.group_expressions.push_back(make_uniq<StarExpression>());
	return result;
}

GroupByNode PEGTransformerFactory::TransformGroupByList(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto group_by_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	GroupByNode result;
	for (auto group_by_expr : group_by_list) {
		result.group_expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(group_by_expr));
	}
	return result;
}

CommonTableExpressionMap PEGTransformerFactory::TransformWithClause(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto recursive_opt = list_pr.Child<OptionalParseResult>(1);
	if (recursive_opt.HasResult()) {
		throw NotImplementedException("Recursive CTEs are not yet implemented");
	}
	auto with_statement_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	CommonTableExpressionMap result;

	for (idx_t entry_idx = 0; entry_idx < with_statement_list.size(); entry_idx++) {
		auto with_entry =
		    transformer.Transform<pair<string, unique_ptr<CommonTableExpressionInfo>>>(with_statement_list[entry_idx]);
		result.map.insert(with_entry.first, std::move(with_entry.second));
	}
	return result;
}

pair<string, unique_ptr<CommonTableExpressionInfo>>
PEGTransformerFactory::TransformWithStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<CommonTableExpressionInfo>();
	auto cte_name = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<vector<string>>(list_pr, 1, result->aliases);
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 2, result->key_targets);
	auto materialized_opt = list_pr.Child<OptionalParseResult>(4);
	if (materialized_opt.HasResult()) {
		// If this has a result, we know it either NEVER or ALWAYS
		bool not_materialized = transformer.Transform<bool>(materialized_opt.optional_result);
		if (not_materialized) {
			result->materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
		} else {
			result->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
		}
	}
	auto table_ref = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(5));
	D_ASSERT(table_ref->type == TableReferenceType::SUBQUERY);
	auto subquery_ref = unique_ptr_cast<TableRef, SubqueryRef>(std::move(table_ref));
	result->query = std::move(subquery_ref->subquery);
	return make_pair(cte_name, std::move(result));
}

bool PEGTransformerFactory::TransformMaterialized(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto not_opt = list_pr.Child<OptionalParseResult>(0);
	return not_opt.HasResult();
}

} // namespace duckdb
