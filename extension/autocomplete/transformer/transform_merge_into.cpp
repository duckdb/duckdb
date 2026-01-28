#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformMergeIntoStatement(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<MergeIntoStatement>();
	transformer.TransformOptional<CommonTableExpressionMap>(list_pr, 0, result->cte_map);
	result->target = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(3));
	result->source = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(4));
	auto join_condition = transformer.Transform<JoinQualifier>(list_pr.Child<ListParseResult>(5));
	if (join_condition.on_clause) {
		result->join_condition = std::move(join_condition.on_clause);
	} else {
		result->using_columns = std::move(join_condition.using_columns);
	}

	auto merge_match_repeat = list_pr.Child<RepeatParseResult>(6);
	map<MergeActionCondition, unique_ptr<MergeIntoAction>> unconditional_actions;
	for (auto merge_match : merge_match_repeat.children) {
		auto merge_match_result =
		    transformer.Transform<pair<MergeActionCondition, unique_ptr<MergeIntoAction>>>(merge_match);
		auto action_condition = merge_match_result.first;
		auto &action = merge_match_result.second;
		if (!action->condition) {
			auto entry = unconditional_actions.find(action_condition);
			if (entry != unconditional_actions.end()) {
				string action_condition_str = MergeIntoStatement::ActionConditionToString(action_condition);
				throw ParserException(
				    "Unconditional %s clause was already defined - only one unconditional %s clause is supported",
				    action_condition_str, action_condition_str);
			}
			unconditional_actions.emplace(action_condition, std::move(action));
			continue;
		}
		result->actions[action_condition].push_back(std::move(action));
	}
	// finally add the unconditional actions
	for (auto &entry : unconditional_actions) {
		result->actions[entry.first].push_back(std::move(entry.second));
	}
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 7, result->returning_list);
	return std::move(result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformMergeIntoUsingClause(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(1));
}

pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
PEGTransformerFactory::TransformMergeMatch(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<pair<MergeActionCondition, unique_ptr<MergeIntoAction>>>(
	    list_pr.Child<ChoiceParseResult>(0).result);
}

pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
PEGTransformerFactory::TransformMatchedClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto condition = MergeActionCondition::WHEN_MATCHED;
	auto merge_into_action = transformer.Transform<unique_ptr<MergeIntoAction>>(list_pr.Child<ListParseResult>(4));
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 2, merge_into_action->condition);
	return pair<MergeActionCondition, unique_ptr<MergeIntoAction>>(condition, std::move(merge_into_action));
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformMatchedClauseAction(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<MergeIntoAction>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformUpdateMatchClause(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	unique_ptr<MergeIntoAction> result;
	auto update_info = list_pr.Child<OptionalParseResult>(1);
	if (update_info.HasResult()) {
		result = transformer.Transform<unique_ptr<MergeIntoAction>>(update_info.optional_result);
	} else {
		result = make_uniq<MergeIntoAction>();
	}
	result->action_type = MergeActionType::MERGE_UPDATE;
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformUpdateMatchInfo(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<MergeIntoAction>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (choice_pr->name == "ByNameOrPosition") {
		result->column_order = transformer.Transform<InsertColumnOrder>(choice_pr);
	} else {
		result->update_info = transformer.Transform<unique_ptr<UpdateSetInfo>>(choice_pr);
	}
	return result;
}

unique_ptr<UpdateSetInfo> PEGTransformerFactory::TransformUpdateMatchSetClause(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &inner_list = list_pr.Child<ListParseResult>(1);
	auto choice_pr = inner_list.Child<ChoiceParseResult>(0).result;
	if (choice_pr->type == ParseResultType::KEYWORD) {
		// We found the '*'
		return nullptr;
	}
	return transformer.Transform<unique_ptr<UpdateSetInfo>>(choice_pr);
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformDeleteMatchClause(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto result = make_uniq<MergeIntoAction>();
	result->action_type = MergeActionType::MERGE_DELETE;
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformInsertMatchClause(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	unique_ptr<MergeIntoAction> result;
	auto insert_info = list_pr.Child<OptionalParseResult>(1);
	if (insert_info.HasResult()) {
		result = transformer.Transform<unique_ptr<MergeIntoAction>>(insert_info.optional_result);
	} else {
		result = make_uniq<MergeIntoAction>();
	}
	result->action_type = MergeActionType::MERGE_INSERT;
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformInsertMatchInfo(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<MergeIntoAction>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformInsertDefaultValues(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto result = make_uniq<MergeIntoAction>();
	result->default_values = true;
	return result;
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformInsertByNameOrPosition(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<MergeIntoAction>();
	transformer.TransformOptional<InsertColumnOrder>(list_pr, 0, result->column_order);
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformInsertValuesList(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<MergeIntoAction>();
	transformer.TransformOptional<vector<string>>(list_pr, 0, result->insert_columns);
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(2));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	for (auto &expr : expr_list) {
		result->expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return result;
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformDoNothingMatchClause(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto result = make_uniq<MergeIntoAction>();
	result->action_type = MergeActionType::MERGE_DO_NOTHING;
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformErrorMatchClause(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<MergeIntoAction>();
	result->action_type = MergeActionType::MERGE_ERROR;
	auto expression_opt = list_pr.Child<OptionalParseResult>(1);
	if (expression_opt.HasResult()) {
		result->expressions.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(expression_opt.optional_result));
	}
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformAndExpression(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
PEGTransformerFactory::TransformNotMatchedClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = transformer.Transform<unique_ptr<MergeIntoAction>>(list_pr.Child<ListParseResult>(6));
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 4, result->condition);
	MergeActionCondition action_condition = MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET;
	transformer.TransformOptional<MergeActionCondition>(list_pr, 3, action_condition);
	return pair<MergeActionCondition, unique_ptr<MergeIntoAction>>(action_condition, std::move(result));
}

MergeActionCondition PEGTransformerFactory::TransformBySourceOrTarget(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<MergeActionCondition>(list_pr.Child<ChoiceParseResult>(0).result);
}
} // namespace duckdb
