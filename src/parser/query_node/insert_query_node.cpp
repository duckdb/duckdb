#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

InsertQueryNode::InsertQueryNode() : QueryNode(QueryNodeType::INSERT_QUERY_NODE) {
}

InsertQueryNode::InsertQueryNode(InsertStatement &stmt)
    : QueryNode(QueryNodeType::INSERT_QUERY_NODE), columns(stmt.columns), default_values(stmt.default_values),
      column_order(stmt.column_order) {
	auto ref = make_uniq<BaseTableRef>();
	ref->table_name = stmt.table;
	ref->schema_name = stmt.schema;
	ref->catalog_name = stmt.catalog;
	table = std::move(ref);
	if (stmt.select_statement) {
		select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(stmt.select_statement->Copy());
	}
	for (auto &expr : stmt.returning_list) {
		returning_list.push_back(expr->Copy());
	}
	if (stmt.on_conflict_info) {
		on_conflict_info = stmt.on_conflict_info->Copy();
	}
	if (stmt.table_ref) {
		table_ref = stmt.table_ref->Copy();
	}
}

string InsertQueryNode::ToString() const {
	// Reconstruct an InsertStatement to reuse its ToString
	InsertStatement stmt;
	if (table) {
		auto &base = table->Cast<BaseTableRef>();
		stmt.table = base.table_name;
		stmt.schema = base.schema_name;
		stmt.catalog = base.catalog_name;
	}
	stmt.columns = columns;
	stmt.default_values = default_values;
	stmt.column_order = column_order;
	if (select_statement) {
		stmt.select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(select_statement->Copy());
	}
	for (auto &expr : returning_list) {
		stmt.returning_list.push_back(expr->Copy());
	}
	if (on_conflict_info) {
		stmt.on_conflict_info = on_conflict_info->Copy();
	}
	if (table_ref) {
		stmt.table_ref = table_ref->Copy();
	}
	return stmt.ToString();
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
