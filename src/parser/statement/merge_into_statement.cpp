#include "duckdb/parser/statement/merge_into_statement.hpp"

namespace duckdb {

MergeIntoStatement::MergeIntoStatement() : SQLStatement(StatementType::MERGE_INTO_STATEMENT) {
}

MergeIntoStatement::MergeIntoStatement(const MergeIntoStatement &other) : SQLStatement(other) {
	target = other.target->Copy();
	source = other.source->Copy();
	join_condition = other.join_condition ? other.join_condition->Copy() : nullptr;
	using_columns = other.using_columns;
	for (auto &entry : other.actions) {
		auto &action_list = actions[entry.first];
		for (auto &action : entry.second) {
			action_list.push_back(action->Copy());
		}
	}
	for (auto &entry : other.returning_list) {
		returning_list.push_back(entry->Copy());
	}
	cte_map = other.cte_map.Copy();
}

string MergeIntoStatement::ActionConditionToString(MergeActionCondition condition) {
	switch (condition) {
	case MergeActionCondition::WHEN_MATCHED:
		return "WHEN MATCHED";
	case MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET:
		return "WHEN NOT MATCHED";
	case MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE:
		return "WHEN NOT MATCHED BY SOURCE";
	default:
		throw InternalException("Unknown match condition");
	}
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
		for (idx_t c = 0; c < using_columns.size(); c++) {
			if (c > 0) {
				result += ", ";
			}
			result += using_columns[c];
		}
		result += ")";
	}
	for (auto &entry : actions) {
		for (auto &action : entry.second) {
			result += " ";
			result += MergeIntoStatement::ActionConditionToString(entry.first);
			result += " ";
			result += action->ToString();
		}
	}
	if (!returning_list.empty()) {
		result += " RETURNING ";
		for (idx_t i = 0; i < returning_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			auto column = returning_list[i]->ToString();
			if (!returning_list[i]->GetAlias().empty()) {
				column +=
				    StringUtil::Format(" AS %s", KeywordHelper::WriteOptionallyQuoted(returning_list[i]->GetAlias()));
			}
			result += column;
		}
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
		if (column_order == InsertColumnOrder::INSERT_BY_NAME) {
			result += "BY NAME ";
		}
		if (update_info) {
			result += update_info->ToString();
		}
		break;
	case MergeActionType::MERGE_DELETE:
		result += "DELETE";
		break;
	case MergeActionType::MERGE_INSERT:
		result += "INSERT ";
		if (column_order == InsertColumnOrder::INSERT_BY_NAME) {
			result += "BY NAME ";
		}
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
		if (expressions.empty()) {
			if (default_values) {
				result += "DEFAULT VALUES";
			} else {
				result += "*";
			}
		} else {
			result += "VALUES (";
			for (idx_t c = 0; c < expressions.size(); c++) {
				if (c > 0) {
					result += ", ";
				}
				result += expressions[c]->ToString();
			}
			result += ")";
		}
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		result += "DO NOTHING";
		break;
	case MergeActionType::MERGE_ERROR:
		result += "ERROR";
		if (!expressions.empty()) {
			result += " " + expressions[0]->ToString();
		}
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
	result->default_values = default_values;
	result->column_order = column_order;
	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	return result;
}

} // namespace duckdb
