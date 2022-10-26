#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"

namespace duckdb {

InsertStatement::InsertStatement() : SQLStatement(StatementType::INSERT_STATEMENT), schema(DEFAULT_SCHEMA) {
}

InsertStatement::InsertStatement(const InsertStatement &other)
    : SQLStatement(other),
      select_statement(unique_ptr_cast<SQLStatement, SelectStatement>(other.select_statement->Copy())),
      columns(other.columns), table(other.table), schema(other.schema) {
	cte_map = other.cte_map.Copy();
}

string InsertStatement::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "INSERT INTO ";
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
