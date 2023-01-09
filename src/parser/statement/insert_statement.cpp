#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/statement/update_statement.hpp"

namespace duckdb {

OnConflictInfo::OnConflictInfo() : action_type(OnConflictAction::THROW) {
}

OnConflictInfo::OnConflictInfo(const OnConflictInfo &other)
    : action_type(other.action_type), indexed_columns(other.indexed_columns) {
	if (other.set_info) {
		set_info = other.set_info->Copy();
	}
}

unique_ptr<OnConflictInfo> OnConflictInfo::Copy() const {
	return unique_ptr<OnConflictInfo>(new OnConflictInfo(*this));
}

InsertStatement::InsertStatement()
    : SQLStatement(StatementType::INSERT_STATEMENT), schema(DEFAULT_SCHEMA), catalog(INVALID_CATALOG) {
}

InsertStatement::InsertStatement(const InsertStatement &other)
    : SQLStatement(other),
      select_statement(unique_ptr_cast<SQLStatement, SelectStatement>(other.select_statement->Copy())),
      columns(other.columns), table(other.table), schema(other.schema), catalog(other.catalog) {
	cte_map = other.cte_map.Copy();
	if (other.on_conflict_info) {
		on_conflict_info = other.on_conflict_info->Copy();
	}
}

string InsertStatement::OnConflictActionToString(OnConflictAction action) {
	switch (action) {
	case OnConflictAction::NOTHING: {
		return "DO NOTHING";
	}
	case OnConflictAction::UPDATE: {
		return "DO UPDATE";
	}
	case OnConflictAction::THROW: {
		// Explicitly left empty, for ToString purposes
		return "";
	}
	default: {
		throw NotImplementedException("type not implemented for OnConflictActionType");
	}
	}
}

// TODO: add ON CONFLICT ... to this
string InsertStatement::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "INSERT INTO ";
	if (!catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
	}
	if (!schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(table);
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
	auto values_list = GetValuesList();
	if (values_list) {
		values_list->alias = string();
		result += values_list->ToString();
	} else {
		result += select_statement->ToString();
	}
	if (on_conflict_info) {
		auto &conflict_info = *on_conflict_info;
		result += " ON CONFLICT ";
		// (optional) conflict target
		if (!conflict_info.indexed_columns.empty()) {
			result += "(";
			auto &columns = conflict_info.indexed_columns;
			for (auto it = columns.begin(); it != columns.end();) {
				result += StringUtil::Lower(*it);
				if (++it != columns.end()) {
					result += ", ";
				}
			}
			result += " )";
		}

		// (optional) where clause
		if (conflict_info.condition) {
			result += " WHERE " + conflict_info.condition->ToString();
		}
		result += " " + OnConflictActionToString(conflict_info.action_type);
		if (conflict_info.set_info) {
			D_ASSERT(conflict_info.action_type == OnConflictAction::UPDATE);
			result += " SET ";
			auto &set_info = *conflict_info.set_info;
			D_ASSERT(set_info.columns.size() == set_info.expressions.size());
			// SET <column_name> = <expression>
			for (idx_t i = 0; i < set_info.columns.size(); i++) {
				auto &column = set_info.columns[i];
				auto &expr = set_info.expressions[i];
				if (i) {
					result += ", ";
				}
				result += StringUtil::Lower(column) + " = " + expr->ToString();
			}
			// (optional) where clause
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
			result += returning_list[i]->ToString();
		}
	}
	return result;
}

unique_ptr<SQLStatement> InsertStatement::Copy() const {
	return unique_ptr<InsertStatement>(new InsertStatement(*this));
}

ExpressionListRef *InsertStatement::GetValuesList() const {
	if (select_statement->node->type != QueryNodeType::SELECT_NODE) {
		return nullptr;
	}
	auto &node = (SelectNode &)*select_statement->node;
	if (node.where_clause || node.qualify || node.having) {
		return nullptr;
	}
	if (!node.cte_map.map.empty()) {
		return nullptr;
	}
	if (!node.groups.grouping_sets.empty()) {
		return nullptr;
	}
	if (node.aggregate_handling != AggregateHandling::STANDARD_HANDLING) {
		return nullptr;
	}
	if (node.select_list.size() != 1 || node.select_list[0]->type != ExpressionType::STAR) {
		return nullptr;
	}
	if (!node.from_table || node.from_table->type != TableReferenceType::EXPRESSION_LIST) {
		return nullptr;
	}
	return (ExpressionListRef *)node.from_table.get();
}

} // namespace duckdb
