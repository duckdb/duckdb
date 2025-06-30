#include "duckdb/parser/statement/merge_into_statement.hpp"

namespace duckdb {

MergeIntoStatement::MergeIntoStatement() : SQLStatement(StatementType::MERGE_INTO_STATEMENT) {
}

MergeIntoStatement::MergeIntoStatement(const MergeIntoStatement &other) : SQLStatement(other) {
	target = other.target->Copy();
	source = other.source->Copy();
	join_condition = other.join_condition ? other.join_condition->Copy() : nullptr;
	using_columns = other.using_columns;
	for (auto &match_action : other.when_matched_actions) {
		when_matched_actions.push_back(match_action->Copy());
	}
	for (auto &match_action : other.when_not_matched_actions) {
		when_not_matched_actions.push_back(match_action->Copy());
	}
	cte_map = other.cte_map.Copy();
}

string MergeIntoStatement::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "MERGE INTO ";
	result += target->ToString();
	result += " USING ";
	result += source->ToString();
	if (join_condition) {
		result += " ON ";
		result += join_condition->ToString();
	} else {
		result += " USING (";
		for(idx_t c = 0; c < using_columns.size(); c++) {
			if (c > 0) {
				result += ", ";
			}
			result += using_columns[c];
		}
		result += ")";
	}
	for (auto &action : when_matched_actions) {
		result += " WHEN MATCHED " + action->ToString();
	}
	for (auto &action : when_not_matched_actions) {
		result += " WHEN NOT MATCHED " + action->ToString();
	}
	return result;
}

unique_ptr<SQLStatement> MergeIntoStatement::Copy() const {
	return unique_ptr<MergeIntoStatement>(new MergeIntoStatement(*this));
}

string MergeIntoAction::ToString() const {
	string result;
	if (condition) {
		result += " AND " + condition->ToString() + " ";
	}
	result += "THEN ";
	switch (action_type) {
	case MergeActionType::MERGE_UPDATE:
		result += "UPDATE ";
		result += update_info->ToString();
		break;
	case MergeActionType::MERGE_DELETE:
		result += "DELETE";
		break;
	case MergeActionType::MERGE_INSERT:
		result += "INSERT ";
		if (!insert_columns.empty()) {
			result += "(";
			for (idx_t c = 0; c < insert_columns.size(); c++) {
				if (c > 0) {
					result += ", ";
				}
				result += KeywordHelper::WriteOptionallyQuoted(insert_columns[c]);
			}
			result += ") ";
		}
		result += "VALUES (";
		for (idx_t c = 0; c < expressions.size(); c++) {
			if (c > 0) {
				result += ", ";
			}
			result += expressions[c]->ToString();
		}
		result += ")";
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		result += "DO NOTHING";
		break;
	case MergeActionType::MERGE_ABORT:
		result += "ABORT";
		break;
	default:
		throw InternalException("Unsupported merge action in ToString");
	}
	return result;
}

unique_ptr<MergeIntoAction> MergeIntoAction::Copy() const {
	auto result = make_uniq<MergeIntoAction>();
	result->action_type = action_type;
	result->condition = condition ? condition->Copy() : nullptr;
	result->update_info = update_info ? update_info->Copy() : nullptr;
	result->insert_columns = insert_columns;
	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	return result;
}

} // namespace duckdb
