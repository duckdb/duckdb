#include "ast/insert_values.hpp"
#include "ast/on_conflict_expression_target.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformInsertStatement(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<InsertStatement>();
	auto &node = *result->node;
	auto &with_opt = list_pr.Child<OptionalParseResult>(0);
	if (with_opt.HasResult()) {
		node.cte_map = transformer.Transform<CommonTableExpressionMap>(with_opt.GetResult());
	}
	auto &or_action_opt = list_pr.Child<OptionalParseResult>(2);
	auto insert_target = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(4));
	// TODO(Dtenwolde) What about the insert alias?
	node.catalog = insert_target->catalog_name;
	node.schema = insert_target->schema_name;
	node.table = insert_target->table_name;
	transformer.TransformOptional<InsertColumnOrder>(list_pr, 5, node.column_order);
	transformer.TransformOptional<vector<string>>(list_pr, 6, node.columns);
	auto insert_values = transformer.Transform<InsertValues>(list_pr.Child<ListParseResult>(7));
	if (!node.columns.empty() && insert_values.default_values) {
		throw ParserException(
		    "You can not provide both a column list and DEFAULT VALUES, please remove one of the two");
	}
	if (insert_values.default_values) {
		node.default_values = true;
	}
	if (insert_values.select_statement) {
		node.select_statement = std::move(insert_values.select_statement);
	}
	auto on_conflict_info = make_uniq<OnConflictInfo>();
	auto &on_conflict_clause = list_pr.Child<OptionalParseResult>(8);
	if (on_conflict_clause.HasResult()) {
		if (or_action_opt.HasResult()) {
			// OR REPLACE | OR IGNORE are shorthands for the ON CONFLICT clause
			throw ParserException("You can not provide both OR REPLACE|IGNORE and an ON CONFLICT clause, please remove "
			                      "the first if you want to have more granual control");
		}
		on_conflict_info = transformer.Transform<unique_ptr<OnConflictInfo>>(on_conflict_clause.GetResult());
		node.on_conflict_info = std::move(on_conflict_info);
		node.table_ref = std::move(insert_target);
	} else if (or_action_opt.HasResult()) {
		on_conflict_info->action_type = transformer.Transform<OnConflictAction>(or_action_opt.GetResult());
		node.on_conflict_info = std::move(on_conflict_info);
		node.table_ref = std::move(insert_target);
	}
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 9, node.returning_list);
	return std::move(result);
}

OnConflictAction PEGTransformerFactory::TransformOrAction(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &replace_or_ignore = list_pr.Child<ChoiceParseResult>(1).GetResult();
	auto &replace_or_ignore_keyword = replace_or_ignore.Cast<KeywordParseResult>().keyword;
	if (StringUtil::CIEquals(replace_or_ignore_keyword, "replace")) {
		return OnConflictAction::REPLACE;
	} else if (StringUtil::CIEquals(replace_or_ignore_keyword, "ignore")) {
		return OnConflictAction::NOTHING;
	} else {
		throw InternalException("Unexpected keyword %s encountered for OrAction", replace_or_ignore_keyword);
	}
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformInsertTarget(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto table_ref = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<string>(list_pr, 1, table_ref->alias);
	return table_ref;
}

string PEGTransformerFactory::TransformInsertAlias(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(1).identifier;
}

unique_ptr<OnConflictInfo> PEGTransformerFactory::TransformOnConflictClause(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = transformer.Transform<unique_ptr<OnConflictInfo>>(list_pr.Child<ListParseResult>(3));
	auto &on_conflict_target_opt = list_pr.Child<OptionalParseResult>(2);
	if (on_conflict_target_opt.HasResult()) {
		auto expression_target = transformer.Transform<OnConflictExpressionTarget>(on_conflict_target_opt.GetResult());
		result->indexed_columns = expression_target.indexed_columns;
		if (expression_target.where_clause) {
			result->condition = std::move(expression_target.where_clause);
		}
	}
	return result;
}

OnConflictExpressionTarget PEGTransformerFactory::TransformOnConflictTarget(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<OnConflictExpressionTarget>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

OnConflictExpressionTarget PEGTransformerFactory::TransformOnConflictExpressionTarget(PEGTransformer &transformer,
                                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	OnConflictExpressionTarget result;
	result.indexed_columns = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 1, result.where_clause);
	return result;
}

OnConflictExpressionTarget PEGTransformerFactory::TransformOnConflictIndexTarget(PEGTransformer &transformer,
                                                                                 ParseResult &parse_result) {
	throw NotImplementedException("ON CONSTRAINT conflict target is not supported yet");
}

unique_ptr<OnConflictInfo> PEGTransformerFactory::TransformOnConflictAction(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<OnConflictInfo>>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<OnConflictInfo> PEGTransformerFactory::TransformOnConflictUpdate(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<OnConflictInfo>();
	result->action_type = OnConflictAction::UPDATE;
	result->set_info = transformer.Transform<unique_ptr<UpdateSetInfo>>(list_pr.Child<ListParseResult>(3));
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 4, result->set_info->condition);
	return result;
}

unique_ptr<OnConflictInfo> PEGTransformerFactory::TransformOnConflictNothing(PEGTransformer &transformer,
                                                                             ParseResult &parse_result) {
	auto result = make_uniq<OnConflictInfo>();
	result->action_type = OnConflictAction::NOTHING;
	return result;
}

InsertValues PEGTransformerFactory::TransformInsertValues(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	InsertValues result;
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.GetResult().name == "DefaultValues") {
		result.default_values = true;
		result.select_statement = nullptr;
		return result;
	}
	if (choice_pr.GetResult().name == "SelectStatementInternal") {
		result.default_values = false;
		result.select_statement = transformer.Transform<unique_ptr<SelectStatement>>(choice_pr.GetResult());
		return result;
	}
	throw InternalException("Unexpected choice in InsertValues statement.");
}

InsertColumnOrder PEGTransformerFactory::TransformByNameOrPosition(PEGTransformer &transformer,
                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<InsertColumnOrder>(list_pr.Child<ChoiceParseResult>(1).GetResult());
}

vector<string> PEGTransformerFactory::TransformInsertColumnList(PEGTransformer &transformer,
                                                                ParseResult &parse_result) {
	// InsertColumnList <- Parens(ColumnList)
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &column_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<vector<string>>(column_list);
}

vector<string> PEGTransformerFactory::TransformColumnList(PEGTransformer &transformer, ParseResult &parse_result) {
	// ColumnList <- List(ColId)
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto column_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<string> result;
	for (auto &column : column_list) {
		result.push_back(transformer.Transform<string>(column));
	}
	return result;
}

vector<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformReturningClause(PEGTransformer &transformer,
                                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
}

} // namespace duckdb
