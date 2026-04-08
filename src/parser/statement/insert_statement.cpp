#include "duckdb/parser/statement/insert_statement.hpp"

#include <vector>

#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

OnConflictInfo::OnConflictInfo() : action_type(OnConflictAction::THROW) {
}

OnConflictInfo::OnConflictInfo(const OnConflictInfo &other)
    : action_type(other.action_type), indexed_columns(other.indexed_columns) {
	if (other.set_info) {
		set_info = other.set_info->Copy();
	}
	if (other.condition) {
		condition = other.condition->Copy();
	}
}

unique_ptr<OnConflictInfo> OnConflictInfo::Copy() const {
	return unique_ptr<OnConflictInfo>(new OnConflictInfo(*this));
}

bool OnConflictInfo::Equals(const unique_ptr<OnConflictInfo> &left, const unique_ptr<OnConflictInfo> &right) {
	if (!left && !right) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	if (left->action_type != right->action_type) {
		return false;
	}
	if (left->indexed_columns != right->indexed_columns) {
		return false;
	}
	if (!UpdateSetInfo::Equals(left->set_info, right->set_info)) {
		return false;
	}
	if (!ParsedExpression::Equals(left->condition, right->condition)) {
		return false;
	}
	return true;
}

InsertStatement::InsertStatement() : SQLStatement(StatementType::INSERT_STATEMENT), node(make_uniq<InsertQueryNode>()) {
}

InsertStatement::InsertStatement(const InsertStatement &other)
    : SQLStatement(other), node(unique_ptr_cast<QueryNode, InsertQueryNode>(other.node->Copy())) {
}

string OnConflictInfo::ActionToString(OnConflictAction action) {
	switch (action) {
	case OnConflictAction::NOTHING:
		return "DO NOTHING";
	case OnConflictAction::REPLACE:
	case OnConflictAction::UPDATE:
		return "DO UPDATE";
	case OnConflictAction::THROW:
		// Explicitly left empty, for ToString purposes
		return "";
	default: {
		throw NotImplementedException("type not implemented for OnConflictActionType");
	}
	}
}

string InsertStatement::ToString() const {
	return node->ToString();
}

unique_ptr<SQLStatement> InsertStatement::Copy() const {
	return unique_ptr<InsertStatement>(new InsertStatement(*this));
}

optional_ptr<ExpressionListRef> InsertStatement::GetValuesList() const {
	return node->GetValuesList();
}

} // namespace duckdb
