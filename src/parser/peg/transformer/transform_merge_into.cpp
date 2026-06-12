#include "duckdb/common/set.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformMergeIntoStatement(
    PEGTransformer &transformer, CommonTableExpressionMap with_clause, unique_ptr<BaseTableRef> target_opt_alias,
    unique_ptr<TableRef> merge_into_using_clause, JoinQualifier join_qualifier,
    vector<pair<MergeActionCondition, unique_ptr<MergeIntoAction>>> merge_match,
    vector<unique_ptr<ParsedExpression>> returning_clause) {
	auto result = make_uniq<MergeIntoStatement>();
	auto &node = *result->node;
	node.cte_map = std::move(with_clause);
	node.target = std::move(target_opt_alias);
	node.source = std::move(merge_into_using_clause);
	if (join_qualifier.on_clause) {
		node.join_condition = std::move(join_qualifier.on_clause);
	} else {
		node.using_columns = join_qualifier.using_columns;
	}

	set<MergeActionCondition> unconditional_actions;
	for (auto &merge_match_result : merge_match) {
		auto action_condition = merge_match_result.first;
		auto &action = merge_match_result.second;
		// Once an unconditional clause has been seen for a condition type, no further clauses
		// of the same type are allowed: they would be unreachable.
		if (unconditional_actions.count(action_condition)) {
			string action_condition_str = MergeQueryNode::ActionConditionToString(action_condition);
			throw ParserException(
			    "Unconditional %s clause was already defined - any following %s clause would be unreachable",
			    action_condition_str, action_condition_str);
		}
		if (!action->condition) {
			unconditional_actions.insert(action_condition);
		}
		node.actions[action_condition].push_back(std::move(action));
	}
	node.returning_list = std::move(returning_clause);
	return std::move(result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformMergeIntoUsingClause(PEGTransformer &transformer,
                                                                          unique_ptr<TableRef> table_ref) {
	return table_ref;
}

pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
PEGTransformerFactory::TransformMatchedClause(PEGTransformer &transformer, unique_ptr<ParsedExpression> and_expression,
                                              unique_ptr<MergeIntoAction> matched_clause_action) {
	matched_clause_action->condition = std::move(and_expression);
	return pair<MergeActionCondition, unique_ptr<MergeIntoAction>>(MergeActionCondition::WHEN_MATCHED,
	                                                               std::move(matched_clause_action));
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformUpdateMatchClause(PEGTransformer &transformer,
                                                  unique_ptr<MergeIntoAction> update_match_info) {
	auto result = std::move(update_match_info);
	if (!result) {
		result = make_uniq<MergeIntoAction>();
	}
	result->action_type = MergeActionType::MERGE_UPDATE;
	return result;
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformUpdateMatchSetAction(PEGTransformer &transformer,
                                                     unique_ptr<UpdateSetInfo> update_match_set_clause) {
	auto result = make_uniq<MergeIntoAction>();
	result->update_info = std::move(update_match_set_clause);
	return result;
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformUpdateByNameOrPosition(PEGTransformer &transformer,
                                                       const InsertColumnOrder &by_name_or_position) {
	auto result = make_uniq<MergeIntoAction>();
	result->column_order = by_name_or_position;
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformDeleteMatchClause(PEGTransformer &transformer) {
	auto result = make_uniq<MergeIntoAction>();
	result->action_type = MergeActionType::MERGE_DELETE;
	return result;
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformInsertMatchClause(PEGTransformer &transformer,
                                                  unique_ptr<MergeIntoAction> insert_match_info) {
	auto result = std::move(insert_match_info);
	if (!result) {
		result = make_uniq<MergeIntoAction>();
	}
	result->action_type = MergeActionType::MERGE_INSERT;
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformInsertDefaultValues(PEGTransformer &transformer) {
	auto result = make_uniq<MergeIntoAction>();
	result->default_values = true;
	return result;
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformInsertByNameOrPosition(PEGTransformer &transformer,
                                                       const InsertColumnOrder &by_name_or_position) {
	auto result = make_uniq<MergeIntoAction>();
	result->column_order = by_name_or_position;
	return result;
}

unique_ptr<MergeIntoAction>
PEGTransformerFactory::TransformInsertValuesList(PEGTransformer &transformer, const vector<string> &insert_column_list,
                                                 vector<unique_ptr<ParsedExpression>> expression) {
	auto result = make_uniq<MergeIntoAction>();
	result->insert_columns = StringsToIdentifiers(insert_column_list);
	result->expressions = std::move(expression);
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformDoNothingMatchClause(PEGTransformer &transformer) {
	auto result = make_uniq<MergeIntoAction>();
	result->action_type = MergeActionType::MERGE_DO_NOTHING;
	return result;
}

unique_ptr<MergeIntoAction> PEGTransformerFactory::TransformErrorMatchClause(PEGTransformer &transformer,
                                                                             unique_ptr<ParsedExpression> expression) {
	auto result = make_uniq<MergeIntoAction>();
	result->action_type = MergeActionType::MERGE_ERROR;
	if (expression) {
		result->expressions.push_back(std::move(expression));
	}
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformAndExpression(PEGTransformer &transformer,
                                                                           unique_ptr<ParsedExpression> expression) {
	return expression;
}

pair<MergeActionCondition, unique_ptr<MergeIntoAction>> PEGTransformerFactory::TransformNotMatchedClause(
    PEGTransformer &transformer, const MergeActionCondition &by_source_or_target,
    unique_ptr<ParsedExpression> and_expression, unique_ptr<MergeIntoAction> matched_clause_action) {
	matched_clause_action->condition = std::move(and_expression);
	auto action_condition = by_source_or_target;
	if (action_condition == MergeActionCondition::WHEN_MATCHED) {
		action_condition = MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET;
	}
	return pair<MergeActionCondition, unique_ptr<MergeIntoAction>>(action_condition, std::move(matched_clause_action));
}

MergeActionCondition PEGTransformerFactory::TransformBySource(PEGTransformer &transformer) {
	return MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE;
}

MergeActionCondition PEGTransformerFactory::TransformByTarget(PEGTransformer &transformer) {
	return MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET;
}

} // namespace duckdb
