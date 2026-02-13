#include "ast/distinct_clause.hpp"
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
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformSelectStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<SelectStatement>
PEGTransformerFactory::TransformSelectStatementInternal(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	CommonTableExpressionMap cte_map;
	transformer.TransformOptional<CommonTableExpressionMap>(list_pr, 0, cte_map);
	if (!cte_map.map.empty()) {
		transformer.stored_cte_map.push_back(cte_map);
	}
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));

	if (!cte_map.map.empty()) {
		select_statement->node->cte_map = std::move(cte_map);
	}
	vector<unique_ptr<ResultModifier>> result_modifiers;
	transformer.TransformOptional<vector<unique_ptr<ResultModifier>>>(list_pr, 2, result_modifiers);
	for (auto &result_modifier : result_modifiers) {
		select_statement->node->modifiers.push_back(std::move(result_modifier));
	}
	return select_statement;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSelectSetOpChain(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(0));
	auto setop_opt = list_pr.Child<OptionalParseResult>(1);
	if (!setop_opt.HasResult()) {
		return select;
	}
	auto setop_repeat = setop_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &setop : setop_repeat.children) {
		auto setop_list = setop->Cast<ListParseResult>();
		auto setop_result = transformer.Transform<unique_ptr<SetOperationNode>>(setop_list.Child<ListParseResult>(0));
		auto right_select = transformer.Transform<unique_ptr<SelectStatement>>(setop_list.Child<ListParseResult>(1));
		setop_result->children.push_back(std::move(select->node));
		setop_result->children.push_back(std::move(right_select->node));
		select->node = std::move(setop_result);
	}
	return select;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformIntersectChain(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto select = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.GetChild(0));
	auto intersect_opt = list_pr.Child<OptionalParseResult>(1);
	if (!intersect_opt.HasResult()) {
		return select;
	}
	auto intersect_repeat = intersect_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &intersect : intersect_repeat.children) {
		auto intersect_list = intersect->Cast<ListParseResult>();
		auto intersect_node = transformer.Transform<unique_ptr<SetOperationNode>>(intersect_list.GetChild(0));
		auto right_select = transformer.Transform<unique_ptr<SelectStatement>>(intersect_list.GetChild(1));
		intersect_node->children.push_back(std::move(select->node));
		intersect_node->children.push_back(std::move(right_select->node));
		select->node = std::move(intersect_node);
	}
	return select;
}

unique_ptr<SetOperationNode>
PEGTransformerFactory::TransformSetIntersectClause(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<SetOperationNode>();
	result->setop_type = SetOperationType::INTERSECT;
	auto is_distinct_opt = list_pr.Child<OptionalParseResult>(1);
	if (is_distinct_opt.HasResult()) {
		result->setop_all = !transformer.Transform<bool>(is_distinct_opt.optional_result);
	}
	return result;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSelectAtom(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SetOperationNode> PEGTransformerFactory::TransformSetopClause(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<SetOperationNode>();
	result->setop_type = transformer.Transform<SetOperationType>(list_pr.Child<ListParseResult>(0));
	auto is_distinct_opt = list_pr.Child<OptionalParseResult>(1);
	if (is_distinct_opt.HasResult()) {
		result->setop_all = !transformer.Transform<bool>(is_distinct_opt.optional_result);
	}
	auto by_name = list_pr.Child<OptionalParseResult>(2);
	if (by_name.HasResult()) {
		if (result->setop_type == SetOperationType::UNION) {
			result->setop_type = SetOperationType::UNION_BY_NAME;
		} else {
			throw ParserException("Invalid combination of %s and BY NAME", EnumUtil::ToString(result->setop_type));
		}
	}
	return result;
}

bool PEGTransformerFactory::TransformDistinctOrAll(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return StringUtil::CIEquals(choice_pr->Cast<KeywordParseResult>().keyword, "distinct");
}

SetOperationType PEGTransformerFactory::TransformSetopType(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<SetOperationType>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSelectParens(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
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
	auto opt_window_clause = list_pr.Child<OptionalParseResult>(4);
	if (opt_window_clause.HasResult()) {
		auto window_functions =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_window_clause.optional_result);
		for (auto &window_func : window_functions) {
			D_ASSERT(!window_func->alias.empty());
			string window_name(window_func->alias);
			window_func->alias = "";
			auto it = transformer.window_clauses.find(window_name);
			if (it != transformer.window_clauses.end()) {
				throw ParserException("window \"%s\" is already defined", window_name);
			}
			auto window_function = unique_ptr_cast<ParsedExpression, WindowExpression>(std::move(window_func));
			transformer.window_clauses[window_name] = std::move(window_function);
		}
	}
	auto select_node = transformer.Transform<unique_ptr<SelectNode>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 1, select_node->where_clause);
	auto group_opt = list_pr.Child<OptionalParseResult>(2);
	if (group_opt.HasResult()) {
		auto group_by_node = transformer.Transform<GroupByNode>(group_opt.optional_result);
		if (group_by_node.group_expressions.size() == 1 && ExpressionIsEmptyStar(*group_by_node.group_expressions[0])) {
			select_node->aggregate_handling = AggregateHandling::FORCE_AGGREGATES;
			group_by_node.group_expressions.clear();
			group_by_node.grouping_sets.clear();
		}
		select_node->groups = std::move(group_by_node);
	}
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, select_node->having);
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 5, select_node->qualify);
	transformer.TransformOptional<unique_ptr<SampleOptions>>(list_pr, 6, select_node->sample);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	transformer.window_clauses.clear();
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
	auto select_node = transformer.Transform<unique_ptr<SelectNode>>(list_pr.Child<ListParseResult>(0));
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
	auto from_table = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(0));
	auto opt_select = list_pr.Child<OptionalParseResult>(1);
	if (opt_select.HasResult()) {
		select_node = transformer.Transform<unique_ptr<SelectNode>>(opt_select.optional_result);
	} else {
		select_node->select_list.push_back(make_uniq<StarExpression>());
	}
	select_node->from_table = std::move(from_table);
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

unique_ptr<SelectNode> PEGTransformerFactory::TransformSelectClause(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<SelectNode>();
	auto opt_distinct = list_pr.Child<OptionalParseResult>(1);
	if (opt_distinct.HasResult()) {
		auto distinct_clause = transformer.Transform<DistinctClause>(opt_distinct.optional_result);
		if (distinct_clause.is_distinct) {
			auto distinct_modifier = make_uniq<DistinctModifier>();
			for (auto &distinct_on : distinct_clause.distinct_targets) {
				distinct_modifier->distinct_on_targets.push_back(distinct_on->Copy());
			}
			result->modifiers.push_back(std::move(distinct_modifier));
		}
	}
	auto target_list = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(2));
	for (auto &expr_ptr : target_list) {
		result->select_list.push_back(std::move(expr_ptr));
	}
	return result;
}

DistinctClause PEGTransformerFactory::TransformDistinctClause(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<DistinctClause>(list_pr.Child<ChoiceParseResult>(0).result);
}

DistinctClause PEGTransformerFactory::TransformDistinctOn(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	DistinctClause result;
	result.is_distinct = true;
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 1, result.distinct_targets);
	return result;
}

DistinctClause PEGTransformerFactory::TransformDistinctAll(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	DistinctClause result;
	result.is_distinct = false;
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformDistinctOnTargets(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> result;
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	for (auto &expr : expr_list) {
		result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return result;
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
				auto &join_ref = transform_join_or_pivot->Cast<JoinRef>();
				join_ref.left = std::move(inner_table_ref);
				inner_table_ref = std::move(transform_join_or_pivot);
			} else if (transform_join_or_pivot->type == TableReferenceType::PIVOT) {
				auto &pivot_ref = transform_join_or_pivot->Cast<PivotRef>();
				pivot_ref.source = std::move(inner_table_ref);
				inner_table_ref = std::move(transform_join_or_pivot);
			} else {
				throw NotImplementedException("Unsupported TableRef type encountered: %s",
				                              EnumUtil::ToString(transform_join_or_pivot->type));
			}
		}
	}

	auto opt_table_alias = list_pr.Child<OptionalParseResult>(2);
	if (opt_table_alias.HasResult()) {
		auto table_alias = transformer.Transform<TableAlias>(opt_table_alias.optional_result);
		auto outer_select = make_uniq<SelectNode>();
		outer_select->from_table = std::move(inner_table_ref);
		outer_select->select_list.push_back(make_uniq<StarExpression>());
		auto select_statement = make_uniq<SelectStatement>();
		select_statement->node = std::move(outer_select);
		auto subquery_ref = make_uniq<SubqueryRef>(std::move(select_statement), table_alias.name);
		subquery_ref->column_name_alias = table_alias.column_name_alias;
		inner_table_ref = std::move(subquery_ref);
	}
	return inner_table_ref;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformJoinOrPivot(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableUnpivotClause(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<PivotRef>();
	transformer.TransformOptional<bool>(list_pr, 1, result->include_nulls);
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(2));
	auto inner_list = extract_parens->Cast<ListParseResult>();
	result->unpivot_names = transformer.Transform<vector<string>>(inner_list.GetChild(0));
	auto pivot_values_list = inner_list.Child<RepeatParseResult>(2);
	for (auto pivot_value : pivot_values_list.children) {
		result->pivots.push_back(transformer.Transform<PivotColumn>(pivot_value));
	}
	if (result->pivots.size() > 1) {
		throw ParserException("UNPIVOT requires a single pivot element");
	}
	TableAlias pivot_alias;
	transformer.TransformOptional<TableAlias>(list_pr, 3, pivot_alias);
	result->alias = pivot_alias.name;
	result->column_name_alias = pivot_alias.column_name_alias;
	return std::move(result);
}

PivotColumn PEGTransformerFactory::TransformUnpivotValueList(PEGTransformer &transformer,
                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	PivotColumn result;
	result.unpivot_names = transformer.Transform<vector<string>>(list_pr.GetChild(0));
	if (result.unpivot_names.size() != 1) {
		throw ParserException("UNPIVOT requires a single column name for the PIVOT IN clause");
	}
	result.entries = transformer.Transform<vector<PivotColumnEntry>>(list_pr.GetChild(2));
	return result;
}

void PEGTransformerFactory::GetValueFromExpression(unique_ptr<ParsedExpression> &expr, vector<Value> &result) {
	if (expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
		auto &const_expr = expr->Cast<ConstantExpression>();
		result.push_back(const_expr.value);
	} else if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref_expr = expr->Cast<ColumnRefExpression>();
		for (auto &col : col_ref_expr.column_names) {
			result.push_back(Value(col));
		}
	} else if (expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func_expr = expr->Cast<FunctionExpression>();
		if (func_expr.function_name == "row") {
			for (auto &col : func_expr.children) {
				GetValueFromExpression(col, result);
			}
		}
	}
}

bool PEGTransformerFactory::TransformPivotInList(unique_ptr<ParsedExpression> &expr, PivotColumnEntry &entry) {
	switch (expr->GetExpressionType()) {
	case ExpressionType::COLUMN_REF: {
		auto &colref = expr->Cast<ColumnRefExpression>();
		if (colref.IsQualified()) {
			throw ParserException(expr->GetQueryLocation(), "PIVOT IN list cannot contain qualified column references");
		}
		entry.values.emplace_back(colref.GetColumnName());
		return true;
	}
	case ExpressionType::FUNCTION: {
		auto &function = expr->Cast<FunctionExpression>();
		if (function.function_name != "row") {
			return false;
		}
		for (auto &child : function.children) {
			if (!TransformPivotInList(child, entry)) {
				return false;
			}
		}
		return true;
	}
	default: {
		Value val;
		if (!ConstructConstantFromExpression(*expr, val)) {
			return false;
		}
		entry.values.push_back(std::move(val));
		return true;
	}
	}
}

vector<PivotColumnEntry> PEGTransformerFactory::TransformUnpivotTargetList(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.GetChild(0));
	auto target_list = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(extract_parens);
	vector<PivotColumnEntry> result;
	for (auto &target : target_list) {
		PivotColumnEntry pivot_entry;
		pivot_entry.alias = target->alias;
		pivot_entry.expr = std::move(target);
		result.push_back(std::move(pivot_entry));
	}
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTablePivotClause(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto inner_list = extract_parens->Cast<ListParseResult>();
	auto result = make_uniq<PivotRef>();
	result->aggregates =
	    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(inner_list.Child<ListParseResult>(0));
	auto pivot_values_list = inner_list.Child<RepeatParseResult>(2);
	for (auto pivot_value : pivot_values_list.children) {
		result->pivots.push_back(transformer.Transform<PivotColumn>(pivot_value));
	}
	transformer.TransformOptional<vector<string>>(inner_list, 3, result->groups);
	TableAlias table_alias;
	transformer.TransformOptional<TableAlias>(list_pr, 2, table_alias);
	result->alias = table_alias.name;
	result->column_name_alias = table_alias.column_name_alias;
	return std::move(result);
}

vector<string> PEGTransformerFactory::TransformPivotGroupByList(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto group_list = ExtractParseResultsFromList(list_pr.GetChild(2));
	vector<string> result;
	for (auto &colid : group_list) {
		result.push_back(transformer.Transform<string>(colid));
	}
	return result;
}

PivotColumn PEGTransformerFactory::TransformPivotValueList(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	PivotColumn result;
	auto pivot_expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &inner_list = list_pr.Child<ListParseResult>(2);
	auto target_list_or_enum = inner_list.Child<ChoiceParseResult>(0).result;
	if (target_list_or_enum->type == ParseResultType::IDENTIFIER) {
		result.pivot_enum = target_list_or_enum->Cast<IdentifierParseResult>().identifier;
	} else {
		result.entries = transformer.Transform<vector<PivotColumnEntry>>(target_list_or_enum);
	}
	if (pivot_expression->GetExpressionClass() != ExpressionClass::FUNCTION) {
		result.pivot_expressions.push_back(std::move(pivot_expression));
		return result;
	}
	auto &func_expr = pivot_expression->Cast<FunctionExpression>();
	if (func_expr.function_name != "row") {
		result.pivot_expressions.push_back(std::move(pivot_expression));
		return result;
	}
	result.pivot_expressions = std::move(func_expr.children);
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPivotHeader(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(0));
}

vector<PivotColumnEntry> PEGTransformerFactory::TransformPivotTargetList(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<PivotColumnEntry> result;
	auto extract_target_list = ExtractResultFromParens(list_pr.GetChild(0));
	auto target_list = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(extract_target_list);
	for (auto &target : target_list) {
		PivotColumnEntry pivot_entry;
		pivot_entry.alias = target->alias;
		bool transformed = TransformPivotInList(target, pivot_entry);
		if (!transformed) {
			// For pivot we throw an exception
			pivot_entry.expr = std::move(target);
		}
		result.push_back(std::move(pivot_entry));
	}
	return result;
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
	auto asof = list_pr.Child<OptionalParseResult>(0).HasResult();
	if (asof) {
		result->ref_type = JoinRefType::ASOF;
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
		auto col_identifier = column->Cast<IdentifierParseResult>().identifier;
		if (col_identifier.empty()) {
			throw ParserException("Column identifier cannot be empty");
		}
		result.using_columns.push_back(col_identifier);
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

	auto qualified_table_function = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(1));
	auto table_function_arguments =
	    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(2));
	result->with_ordinality = list_pr.Child<OptionalParseResult>(3).HasResult() ? OrdinalityType::WITH_ORDINALITY
	                                                                            : OrdinalityType::WITHOUT_ORDINALITY;
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
	result->with_ordinality = list_pr.Child<OptionalParseResult>(3).HasResult() ? OrdinalityType::WITH_ORDINALITY
	                                                                            : OrdinalityType::WITHOUT_ORDINALITY;
	result->function =
	    make_uniq<FunctionExpression>(qualified_table_function.catalog, qualified_table_function.schema,
	                                  qualified_table_function.name, std::move(table_function_arguments));
	result->alias = table_alias;
	transformer.TransformOptional<unique_ptr<SampleOptions>>(list_pr, 4, result->sample);
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
	if (!result.catalog.empty() && result.schema.empty()) {
		result.schema = result.catalog;
		result.catalog = INVALID_CATALOG;
	}
	result.name = list_pr.Child<IdentifierParseResult>(2).identifier;
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableSubquery(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
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
	transformer.TransformOptional<unique_ptr<SampleOptions>>(list_pr, 4, result->sample);
	return std::move(result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformParensTableRef(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto table_ref = transformer.Transform<unique_ptr<TableRef>>(extract_parens);
	transformer.TransformOptional<string>(list_pr, 0, table_ref->alias);
	transformer.TransformOptional<unique_ptr<SampleOptions>>(list_pr, 2, table_ref->sample);
	return table_ref;
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

unique_ptr<ResultModifier> PEGTransformerFactory::TransformLimitOffset(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ResultModifier>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ResultModifier> PEGTransformerFactory::VerifyLimitOffset(LimitPercentResult &limit,
                                                                    LimitPercentResult &offset) {
	if (offset.is_percent) {
		throw ParserException("Percentage for offsets are not supported.");
	}
	if (limit.is_percent) {
		auto result = make_uniq<LimitPercentModifier>();
		result->limit = std::move(limit.expression);
		result->offset = std::move(offset.expression);
		return std::move(result);
	}
	auto result = make_uniq<LimitModifier>();
	if (limit.expression) {
		result->limit = std::move(limit.expression);
	}
	if (offset.expression) {
		result->offset = std::move(offset.expression);
	}
	if (!result->limit && !result->offset) {
		return nullptr;
	}
	return std::move(result);
}

unique_ptr<ResultModifier> PEGTransformerFactory::TransformOffsetLimitClause(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto offset = transformer.Transform<LimitPercentResult>(list_pr.Child<ListParseResult>(0));
	LimitPercentResult limit;
	transformer.TransformOptional<LimitPercentResult>(list_pr, 1, limit);
	return VerifyLimitOffset(limit, offset);
}

unique_ptr<ResultModifier> PEGTransformerFactory::TransformLimitOffsetClause(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto limit = transformer.Transform<LimitPercentResult>(list_pr.Child<ListParseResult>(0));
	LimitPercentResult offset;
	transformer.TransformOptional<LimitPercentResult>(list_pr, 1, offset);
	return VerifyLimitOffset(limit, offset);
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

struct GroupingExpressionMap {
	parsed_expression_map_t<idx_t> map;
};

GroupByNode PEGTransformerFactory::TransformGroupByExpressions(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = transformer.Transform<GroupByNode>(list_pr.Child<ChoiceParseResult>(0).result);
	return result;
}

GroupByNode PEGTransformerFactory::TransformGroupByAll(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	GroupByNode result;
	result.group_expressions.push_back(make_uniq<StarExpression>());
	return result;
}

static void CheckGroupingSetMax(idx_t count) {
	static constexpr const idx_t MAX_GROUPING_SETS = 65535;
	if (count > MAX_GROUPING_SETS) {
		throw ParserException("Maximum grouping set count of %d exceeded", MAX_GROUPING_SETS);
	}
}

static void CheckGroupingSetCubes(idx_t current_count, idx_t cube_count) {
	idx_t combinations = 1;
	for (idx_t i = 0; i < cube_count; i++) {
		combinations *= 2;
		CheckGroupingSetMax(current_count + combinations);
	}
}

static GroupingSet VectorToGroupingSet(vector<idx_t> &indexes) {
	GroupingSet result;
	for (idx_t i = 0; i < indexes.size(); i++) {
		result.insert(indexes[i]);
	}
	return result;
}

void PEGTransformerFactory::AddGroupByExpression(unique_ptr<ParsedExpression> expression, GroupingExpressionMap &map,
                                                 GroupByNode &result, vector<idx_t> &result_set) {
	if (expression->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &func = expression->Cast<FunctionExpression>();
		if (func.function_name == "row") {
			for (auto &child : func.children) {
				AddGroupByExpression(std::move(child), map, result, result_set);
			}
			return;
		}
	}
	auto entry = map.map.find(*expression);
	idx_t result_idx;
	if (entry == map.map.end()) {
		result_idx = result.group_expressions.size();
		map.map[*expression] = result_idx;
		result.group_expressions.push_back(std::move(expression));
	} else {
		result_idx = entry->second;
	}
	result_set.push_back(result_idx);
}

static void AddCubeSets(const GroupingSet &current_set, vector<GroupingSet> &cube_sets,
                        vector<GroupingSet> &result_sets, idx_t start_idx = 0) {
	CheckGroupingSetMax(result_sets.size());
	result_sets.push_back(current_set);
	for (idx_t k = start_idx; k < cube_sets.size(); k++) {
		auto child_set = current_set;
		child_set.insert(cube_sets[k].begin(), cube_sets[k].end());
		AddCubeSets(child_set, cube_sets, result_sets, k + 1);
	}
}

vector<GroupingSet> PEGTransformerFactory::GroupByExpressionUnfolding(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> group_by_expr,
                                                                      GroupingExpressionMap &map, GroupByNode &result) {
	vector<GroupingSet> result_sets;
	if (StringUtil::CIEquals(group_by_expr->name, "EmptyGroupingItem")) {
		result_sets.emplace_back();

	} else if (StringUtil::CIEquals(group_by_expr->name, "Expression")) {
		vector<idx_t> indexes;
		auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(group_by_expr);
		AddGroupByExpression(std::move(expr), map, result, indexes);
		result_sets.push_back(VectorToGroupingSet(indexes));
	} else if (StringUtil::CIEquals(group_by_expr->name, "GroupingSetsClause")) {
		auto grouping_set_list = group_by_expr->Cast<ListParseResult>();
		auto inner_group_by_list = ExtractResultFromParens(grouping_set_list.Child<ListParseResult>(2));
		auto &list_pr = inner_group_by_list->Cast<ListParseResult>();
		auto group_by_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
		for (auto &child_wrapper : group_by_list) {
			auto child_list_pr = child_wrapper->Cast<ListParseResult>();
			auto child_expr = child_list_pr.Child<ChoiceParseResult>(0).result;
			auto child_sets = GroupByExpressionUnfolding(transformer, child_expr, map, result);
			result_sets.insert(result_sets.end(), child_sets.begin(), child_sets.end());
		}
	} else if (StringUtil::CIEquals(group_by_expr->name, "CubeOrRollupClause")) {
		auto group_by_list = group_by_expr->Cast<ListParseResult>();
		auto type_str = transformer.Transform<string>(group_by_list.Child<ListParseResult>(0));
		auto extract_parens = ExtractResultFromParens(group_by_list.Child<ListParseResult>(1));
		if (!extract_parens->Cast<OptionalParseResult>().HasResult()) {
			throw ParserException("CUBE or ROLLUP column list cannot be emptied");
		}
		auto expr_list = ExtractParseResultsFromList(extract_parens->Cast<OptionalParseResult>().optional_result);

		vector<GroupingSet> unfolding_sets;
		for (auto &expr_node : expr_list) {
			vector<idx_t> indexes;
			auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(expr_node);
			AddGroupByExpression(std::move(expr), map, result, indexes);

			GroupingSet s;
			for (auto idx : indexes) {
				s.insert(idx);
			}
			unfolding_sets.push_back(std::move(s));
		}

		if (StringUtil::CIEquals(type_str, "CUBE")) {
			CheckGroupingSetCubes(result_sets.size(), unfolding_sets.size());
			GroupingSet current_set;
			AddCubeSets(current_set, unfolding_sets, result_sets, 0);
		} else if (StringUtil::CIEquals(type_str, "ROLLUP")) {
			GroupingSet current_set;
			result_sets.push_back(current_set);
			for (idx_t i = 0; i < unfolding_sets.size(); i++) {
				current_set.insert(unfolding_sets[i].begin(), unfolding_sets[i].end());
				result_sets.push_back(current_set);
			}
		}
	}
	return result_sets;
}

string PEGTransformerFactory::TransformCubeOrRollup(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

GroupByNode PEGTransformerFactory::TransformGroupByList(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto group_by_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));

	GroupByNode result;
	GroupingExpressionMap map;

	for (auto group_by_child : group_by_list) {
		auto group_by_expr_child_list = group_by_child->Cast<ListParseResult>();
		auto group_by_expr = group_by_expr_child_list.Child<ChoiceParseResult>(0).result;

		vector<GroupingSet> next_sets = GroupByExpressionUnfolding(transformer, group_by_expr, map, result);

		if (result.grouping_sets.empty()) {
			result.grouping_sets = std::move(next_sets);
		} else {
			vector<GroupingSet> new_sets;
			idx_t grouping_set_count = result.grouping_sets.size() * next_sets.size();
			CheckGroupingSetMax(grouping_set_count);
			new_sets.reserve(grouping_set_count);

			for (auto &current_set : result.grouping_sets) {
				for (auto &next_set : next_sets) {
					GroupingSet combined_set;
					combined_set.insert(current_set.begin(), current_set.end());
					combined_set.insert(next_set.begin(), next_set.end());
					new_sets.push_back(std::move(combined_set));
				}
			}
			result.grouping_sets = std::move(new_sets);
		}
	}
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformGroupByExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEmptyGroupingItem(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("Rule 'EmptyGroupingItem' has not been implemented yet");
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformCubeOrRollupClause(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("Rule 'CubeOrRollupClause' has not been implemented yet");
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformGroupingSetsClause(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("Rule 'GroupingSetsClause' has not been implemented yet");
}

CommonTableExpressionMap PEGTransformerFactory::TransformWithClause(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool is_recursive = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto with_statement_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	CommonTableExpressionMap result;

	for (idx_t entry_idx = 0; entry_idx < with_statement_list.size(); entry_idx++) {
		auto with_entry =
		    transformer.Transform<pair<string, unique_ptr<CommonTableExpressionInfo>>>(with_statement_list[entry_idx]);

		if (is_recursive) {
			auto &query_node = with_entry.second->query->node;
			if (!query_node->modifiers.empty()) {
				for (auto &modifier : query_node->modifiers) {
					if (modifier->type == ResultModifierType::LIMIT_MODIFIER ||
					    modifier->type == ResultModifierType::LIMIT_PERCENT_MODIFIER) {
						throw ParserException("LIMIT or OFFSET in a recursive query is not allowed");
					}
					if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
						throw ParserException("ORDER BY in a recursive query is not allowed");
					}
				}
			}
			// Now safe to call on SELECT, VALUES, etc.
			query_node = ToRecursiveCTE(std::move(query_node), with_entry.first, with_entry.second->aliases,
			                            with_entry.second->key_targets);
		}
		auto cte_name = string(with_entry.first);

		auto it = result.map.find(cte_name);
		if (it != result.map.end()) {
			// can't have two CTEs with same name
			throw ParserException("Duplicate CTE name \"%s\"", cte_name);
		}
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
		// If this has a result, we know it is either NEVER or ALWAYS
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

vector<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformUsingKey(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto col_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(2));
	vector<unique_ptr<ParsedExpression>> results;
	for (auto col : col_list) {
		results.push_back(make_uniq<ColumnRefExpression>(std::move(col)));
	}
	return results;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformHavingClause(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

LimitPercentResult PEGTransformerFactory::TransformOffsetValue(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	LimitPercentResult result;
	result.expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformQualifyClause(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformWindowClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto window_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(1));
	vector<unique_ptr<ParsedExpression>> result;
	for (auto &window : window_list) {
		result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(window));
	}
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformWindowDefinition(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto window_function = transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ListParseResult>(2));
	window_function->alias = list_pr.Child<IdentifierParseResult>(0).identifier;
	return std::move(window_function);
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleClause(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SampleOptions>>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleEntry(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SampleOptions>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleEntryFunction(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto result = transformer.Transform<unique_ptr<SampleOptions>>(extract_parens);
	transformer.TransformOptional<SampleMethod>(list_pr, 0, result->method);
	auto repeatable_sample_opt = list_pr.Child<OptionalParseResult>(2);
	if (repeatable_sample_opt.HasResult()) {
		auto repeatable_seed = transformer.Transform<optional_idx>(repeatable_sample_opt.optional_result);
		result->seed = repeatable_seed;
		result->repeatable = true;
	}
	return result;
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleEntryCount(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto sample_count = transformer.Transform<unique_ptr<SampleOptions>>(list_pr.Child<ListParseResult>(0));
	auto optional_properties = list_pr.Child<OptionalParseResult>(1);
	if (optional_properties.HasResult()) {
		auto extract_parens = ExtractResultFromParens(optional_properties.optional_result)->Cast<ListParseResult>();
		auto properties = transformer.Transform<pair<SampleMethod, optional_idx>>(extract_parens);
		sample_count->method = properties.first;
		sample_count->seed = properties.second;
	}
	return sample_count;
}

SampleMethod PEGTransformerFactory::TransformSampleFunction(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto method = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	return EnumUtil::FromString<SampleMethod>(method);
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleCount(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<SampleOptions>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	if (expr->GetExpressionClass() != ExpressionClass::CONSTANT) {
		throw ParserException(expr->GetQueryLocation(), "Only constants are supported in sample clause currently");
	}
	auto &const_expr = expr->Cast<ConstantExpression>();
	auto &sample_value = const_expr.value;
	transformer.TransformOptional<bool>(list_pr, 1, result->is_percentage);
	if (result->is_percentage) {
		// sample size is given in sample_size: use system sampling
		auto percentage = sample_value.GetValue<double>();
		if (percentage < 0 || percentage > 100) {
			throw ParserException("Sample sample_size %llf out of range, must be between 0 and 100", percentage);
		}
		result->sample_size = Value::DOUBLE(percentage);
		result->method = SampleMethod::SYSTEM_SAMPLE;
	} else {
		// sample size is given in rows: use reservoir sampling
		auto rows = sample_value.GetValue<int64_t>();
		if (rows < 0 || sample_value.GetValue<uint64_t>() > SampleOptions::MAX_SAMPLE_ROWS) {
			throw ParserException("Sample rows %lld out of range, must be between 0 and %lld", rows,
			                      SampleOptions::MAX_SAMPLE_ROWS);
		}
		result->sample_size = Value::BIGINT(rows);
		result->method = SampleMethod::RESERVOIR_SAMPLE;
	}
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSampleValue(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

bool PEGTransformerFactory::TransformSampleUnit(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0).result);
}

pair<SampleMethod, optional_idx>
PEGTransformerFactory::TransformSampleProperties(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto sample_str = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto sample_method = EnumUtil::FromString<SampleMethod>(sample_str);
	auto seed_opt = list_pr.Child<OptionalParseResult>(1);
	optional_idx seed = optional_idx::Invalid();
	if (seed_opt.HasResult()) {
		auto inner_list = seed_opt.optional_result->Cast<ListParseResult>();
		seed = transformer.Transform<optional_idx>(inner_list.Child<ListParseResult>(1));
	}
	return make_pair(sample_method, seed);
}

optional_idx PEGTransformerFactory::TransformRepeatableSample(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.GetChild(1));
	return transformer.Transform<optional_idx>(extract_parens);
}

optional_idx PEGTransformerFactory::TransformSampleSeed(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(0));
	auto const_expr = expr->Cast<ConstantExpression>();
	return optional_idx(const_expr.value.GetValue<idx_t>());
}

} // namespace duckdb
