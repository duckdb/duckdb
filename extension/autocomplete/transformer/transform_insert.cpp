#include "ast/insert_values.hpp"
#include "ast/on_conflict_expression_target.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformInsertStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	throw NotImplementedException("TransformInsertStatement");
	auto result = make_uniq<InsertStatement>();
	auto with_opt = list_pr.Child<OptionalParseResult>(0);
	if (with_opt.HasResult()) {
		throw NotImplementedException("WITH clause in INSERT statement is not yet supported.");
	}
	auto or_action_opt = list_pr.Child<OptionalParseResult>(2);
	auto insert_target = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(4));
	// TODO(Dtenwolde) What about the insert alias?
	result->catalog = insert_target->catalog_name;
	result->schema = insert_target->schema_name;
	result->table = insert_target->table_name;
	transformer.TransformOptional<InsertColumnOrder>(list_pr, 5, result->column_order);
	transformer.TransformOptional<vector<string>>(list_pr, 6, result->columns);
	auto insert_values = transformer.Transform<InsertValues>(list_pr.Child<ListParseResult>(7));
	if (insert_values.default_values) {
		throw NotImplementedException("DEFAULT Values for insert statement are not yet implemented.");
	}
	if (insert_values.select_statement) {
		result->select_statement = std::move(insert_values.select_statement);
	}
	transformer.TransformOptional<unique_ptr<OnConflictInfo>>(list_pr, 8, result->on_conflict_info);
	if (result->on_conflict_info) {
		transformer.TransformOptional(list_pr, 2, result->on_conflict_info->action_type);
	}
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 9, result->returning_list);

	return std::move(result);
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformInsertTarget(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table_ref = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<string>(list_pr, 1, table_ref->alias);
	return table_ref;
}

unique_ptr<OnConflictInfo> PEGTransformerFactory::TransformOnConflictClause(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<OnConflictInfo>();
	result->action_type = transformer.Transform<OnConflictAction>(list_pr.Child<ListParseResult>(3));
	// TODO(Dtenwolde) Leaving DO UPDATE SET for later
	auto on_conflict_target_opt = list_pr.Child<OptionalParseResult>(2);
	if (on_conflict_target_opt.HasResult()) {
		auto expression_target =
		    transformer.Transform<OnConflictExpressionTarget>(on_conflict_target_opt.optional_result);
		result->indexed_columns = expression_target.indexed_columns;
		result->condition = std::move(expression_target.where_clause);
	}
	return result;
}

OnConflictExpressionTarget
PEGTransformerFactory::TransformOnConflictExpressionTarget(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	OnConflictExpressionTarget result;
	result.indexed_columns = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 1, result.where_clause);
	return result;
}

OnConflictAction PEGTransformerFactory::TransformOnConflictAction(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<OnConflictAction>(list_pr.Child<ChoiceParseResult>(0).result);
}

OnConflictAction PEGTransformerFactory::TransformOnConflictUpdate(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("Rule 'OnConflictUpdate' has not been implemented yet");
}

OnConflictAction PEGTransformerFactory::TransformOnConflictNothing(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	return OnConflictAction::NOTHING;
}

InsertValues PEGTransformerFactory::TransformInsertValues(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	InsertValues result;
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->name == "DefaultValues") {
		result.default_values = true;
		result.select_statement = nullptr;
		return result;
	} else if (choice_pr.result->name == "SelectStatementInternal") {
		result.default_values = false;
		result.select_statement = transformer.Transform<unique_ptr<SelectStatement>>(choice_pr.result);
		return result;
	} else {
		throw InternalException("Unexpected choice in InsertValues statement.");
	}
}

InsertColumnOrder PEGTransformerFactory::TransformByNameOrPosition(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<InsertColumnOrder>(list_pr.Child<ChoiceParseResult>(1).result);
}

vector<string> PEGTransformerFactory::TransformInsertColumnList(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	// InsertColumnList <- Parens(ColumnList)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<vector<string>>(column_list);
}

vector<string> PEGTransformerFactory::TransformColumnList(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	// ColumnList <- List(ColId)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<string> result;
	for (auto &column : column_list) {
		result.push_back(transformer.Transform<string>(column));
	}
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReturningClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
}

} // namespace duckdb
