#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"

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

InsertQueryNode::InsertQueryNode() : QueryNode(QueryNodeType::INSERT_QUERY_NODE) {
}

static optional_ptr<ExpressionListRef> GetValuesListFromSelect(const SelectStatement *select) {
	if (!select) {
		return nullptr;
	}
	if (select->node->type != QueryNodeType::SELECT_NODE) {
		return nullptr;
	}
	auto &sel = select->node->Cast<SelectNode>();
	if (sel.where_clause || sel.qualify || sel.having) {
		return nullptr;
	}
	if (!sel.cte_map.map.empty() || !sel.groups.grouping_sets.empty()) {
		return nullptr;
	}
	if (sel.aggregate_handling != AggregateHandling::STANDARD_HANDLING) {
		return nullptr;
	}
	if (sel.select_list.size() != 1 || sel.select_list[0]->GetExpressionType() != ExpressionType::STAR) {
		return nullptr;
	}
	if (!sel.from_table || sel.from_table->type != TableReferenceType::EXPRESSION_LIST) {
		return nullptr;
	}
	return &sel.from_table->Cast<ExpressionListRef>();
}

optional_ptr<ExpressionListRef> InsertQueryNode::GetValuesList() const {
	return GetValuesListFromSelect(select_statement.get());
}

string InsertQueryNode::OnConflictActionToString(OnConflictAction action) {
	switch (action) {
	case OnConflictAction::NOTHING:
		return "DO NOTHING";
	case OnConflictAction::REPLACE:
	case OnConflictAction::UPDATE:
		return "DO UPDATE";
	case OnConflictAction::THROW:
		return "";
	default:
		throw NotImplementedException("type not implemented for OnConflictActionType");
	}
}

string InsertQueryNode::ToString() const {
	bool or_replace_shorthand_set = false;
	string result;

	result = cte_map.ToString();
	result += "INSERT";
	if (on_conflict_info && on_conflict_info->action_type == OnConflictAction::REPLACE) {
		or_replace_shorthand_set = true;
		result += " OR REPLACE";
	}
	result += " INTO ";
	if (table) {
		auto &base = table->Cast<BaseTableRef>();
		if (!base.catalog_name.empty()) {
			result += KeywordHelper::WriteOptionallyQuoted(base.catalog_name) + ".";
		}
		if (!base.schema_name.empty()) {
			result += KeywordHelper::WriteOptionallyQuoted(base.schema_name) + ".";
		}
		result += KeywordHelper::WriteOptionallyQuoted(base.table_name);
	}
	// Write the (optional) alias of the insert target
	if (table_ref && !table_ref->alias.empty()) {
		result += StringUtil::Format(" AS %s", KeywordHelper::WriteOptionallyQuoted(table_ref->alias));
	}
	if (column_order == InsertColumnOrder::INSERT_BY_NAME) {
		result += " BY NAME";
	}
	if (!columns.empty()) {
		result += " (";
		for (idx_t i = 0; i < columns.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += KeywordHelper::WriteOptionallyQuoted(columns[i]);
		}
		result += " )";
	}
	result += " ";
	auto values_list = GetValuesListFromSelect(select_statement.get());
	if (values_list) {
		D_ASSERT(!default_values);
		auto saved_alias = values_list->alias;
		values_list->alias = string();
		result += values_list->ToString();
		values_list->alias = saved_alias;
	} else if (select_statement) {
		D_ASSERT(!default_values);
		result += select_statement->ToString();
	} else {
		D_ASSERT(default_values);
		result += "DEFAULT VALUES";
	}
	if (!or_replace_shorthand_set && on_conflict_info) {
		auto &conflict_info = *on_conflict_info;
		result += " ON CONFLICT ";
		if (!conflict_info.indexed_columns.empty()) {
			result += "(";
			auto &cols = conflict_info.indexed_columns;
			for (auto it = cols.begin(); it != cols.end();) {
				result += StringUtil::Lower(*it);
				if (++it != cols.end()) {
					result += ", ";
				}
			}
			result += " )";
		}
		if (conflict_info.condition) {
			result += " WHERE " + conflict_info.condition->ToString();
		}
		result += " " + InsertQueryNode::OnConflictActionToString(conflict_info.action_type);
		if (conflict_info.set_info) {
			D_ASSERT(conflict_info.action_type == OnConflictAction::UPDATE);
			result += " SET ";
			auto &set_info = *conflict_info.set_info;
			D_ASSERT(set_info.columns.size() == set_info.expressions.size());
			for (idx_t i = 0; i < set_info.columns.size(); i++) {
				if (i) {
					result += ", ";
				}
				result += StringUtil::Lower(set_info.columns[i]) + " = " + set_info.expressions[i]->ToString();
			}
			if (set_info.condition) {
				result += " WHERE " + set_info.condition->ToString();
			}
		}
	}
	if (!returning_list.empty()) {
		result += " RETURNING ";
		for (idx_t i = 0; i < returning_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			auto col = returning_list[i]->ToString();
			if (!returning_list[i]->GetAlias().empty()) {
				col += StringUtil::Format(" AS %s",
				                         KeywordHelper::WriteOptionallyQuoted(returning_list[i]->GetAlias()));
			}
			result += col;
		}
	}
	return result;
}

bool InsertQueryNode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto &other = other_p->Cast<InsertQueryNode>();
	if (!TableRef::Equals(table, other.table)) {
		return false;
	}
	if (columns != other.columns) {
		return false;
	}
	if (default_values != other.default_values || column_order != other.column_order) {
		return false;
	}
	// compare select_statement
	if ((select_statement == nullptr) != (other.select_statement == nullptr)) {
		return false;
	}
	if (select_statement && !select_statement->Equals(*other.select_statement)) {
		return false;
	}
	// compare returning_list
	if (returning_list.size() != other.returning_list.size()) {
		return false;
	}
	for (idx_t i = 0; i < returning_list.size(); i++) {
		if (!ParsedExpression::Equals(returning_list[i], other.returning_list[i])) {
			return false;
		}
	}
	// compare on_conflict_info fields (no Equals on OnConflictInfo)
	if ((on_conflict_info == nullptr) != (other.on_conflict_info == nullptr)) {
		return false;
	}
	if (on_conflict_info) {
		if (on_conflict_info->action_type != other.on_conflict_info->action_type) {
			return false;
		}
		if (on_conflict_info->indexed_columns != other.on_conflict_info->indexed_columns) {
			return false;
		}
		if (!ParsedExpression::Equals(on_conflict_info->condition, other.on_conflict_info->condition)) {
			return false;
		}
		// compare set_info inside on_conflict_info
		if ((on_conflict_info->set_info == nullptr) != (other.on_conflict_info->set_info == nullptr)) {
			return false;
		}
		if (on_conflict_info->set_info) {
			auto &lsi = *on_conflict_info->set_info;
			auto &rsi = *other.on_conflict_info->set_info;
			if (lsi.columns != rsi.columns) {
				return false;
			}
			if (lsi.expressions.size() != rsi.expressions.size()) {
				return false;
			}
			for (idx_t i = 0; i < lsi.expressions.size(); i++) {
				if (!ParsedExpression::Equals(lsi.expressions[i], rsi.expressions[i])) {
					return false;
				}
			}
			if (!ParsedExpression::Equals(lsi.condition, rsi.condition)) {
				return false;
			}
		}
	}
	// compare table_ref
	if (!TableRef::Equals(table_ref, other.table_ref)) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> InsertQueryNode::Copy() const {
	auto result = make_uniq<InsertQueryNode>();
	if (table) {
		result->table = table->Copy();
	}
	result->columns = columns;
	result->default_values = default_values;
	result->column_order = column_order;
	if (select_statement) {
		result->select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(select_statement->Copy());
	}
	for (auto &expr : returning_list) {
		result->returning_list.push_back(expr->Copy());
	}
	if (on_conflict_info) {
		result->on_conflict_info = on_conflict_info->Copy();
	}
	if (table_ref) {
		result->table_ref = table_ref->Copy();
	}
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
