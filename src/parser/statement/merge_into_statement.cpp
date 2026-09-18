#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"

namespace duckdb {

MergeIntoStatement::MergeIntoStatement()
    : SQLStatement(StatementType::MERGE_INTO_STATEMENT), node(make_uniq<MergeQueryNode>()) {
}

MergeIntoStatement::MergeIntoStatement(const MergeIntoStatement &other)
    : SQLStatement(other), node(unique_ptr_cast<QueryNode, MergeQueryNode>(other.node->Copy())) {
}

string MergeIntoStatement::ToString() const {
	return node->ToString();
}

unique_ptr<SQLStatement> MergeIntoStatement::Copy() const {
	return unique_ptr<MergeIntoStatement>(new MergeIntoStatement(*this));
}

bool MergeIntoAction::Equals(const MergeIntoAction &left, const MergeIntoAction &right) {
	if (left.action_type != right.action_type) {
		return false;
	}
	if (!ParsedExpression::Equals(left.condition, right.condition)) {
		return false;
	}
	if (!UpdateSetInfo::Equals(left.update_info, right.update_info)) {
		return false;
	}
	if (left.insert_columns != right.insert_columns) {
		return false;
	}
	if (left.column_order != right.column_order) {
		return false;
	}
	if (left.default_values != right.default_values) {
		return false;
	}
	if (left.exclude_columns != right.exclude_columns) {
		return false;
	}
	if (left.expressions.size() != right.expressions.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.expressions.size(); i++) {
		if (!ParsedExpression::Equals(left.expressions[i], right.expressions[i])) {
			return false;
		}
	}
	return true;
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
				result += SQLIdentifier(insert_columns[c]);
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
