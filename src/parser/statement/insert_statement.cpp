#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"

namespace duckdb {

InsertStatement::InsertStatement()
    : SQLStatement(StatementType::INSERT_STATEMENT), schema(DEFAULT_SCHEMA), catalog(INVALID_CATALOG) {
}

InsertStatement::InsertStatement(const InsertStatement &other)
    : SQLStatement(other),
      select_statement(unique_ptr_cast<SQLStatement, SelectStatement>(other.select_statement->Copy())),
      columns(other.columns), table(other.table), schema(other.schema), catalog(other.catalog) {
	cte_map = other.cte_map.Copy();
}

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

unique_ptr<SelectStatement> select_statement;
//! Column names to insert into
vector<string> columns;

//! Table name to insert to
string table;
//! Schema name to insert to
string schema;
//! The catalog name to insert to
string catalog;

//! keep track of optional returningList if statement contains a RETURNING keyword
vector<unique_ptr<ParsedExpression>> returning_list;

//! CTEs
CommonTableExpressionMap cte_map;

bool InsertStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const InsertStatement &)*other_p;

	if (columns != other.columns) {
		return false;
	}

	if (!select_statement && !other.select_statement) {

	} else if (!select_statement || !other.select_statement) {
		return false;
	} else if (!select_statement->Equals(other.select_statement.get())) {
		return false;
	}

	if (table != other.table) {
		return false;
	}
	if (schema != other.schema) {
		return false;
	}
	if (catalog != other.catalog) {
		return false;
	}

	if (returning_list.size() != other.returning_list.size()) {
		return false;
	}
	for (idx_t i = 0; i < returning_list.size(); i++) {
		auto &lhs = returning_list[i];
		auto &rhs = other.returning_list[i];

		if (!lhs->Equals(*rhs)) {
			return false;
		}
	}

	return cte_map.Equals(other.cte_map);
}

} // namespace duckdb
