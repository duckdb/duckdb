#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {

DeleteStatement::DeleteStatement() : SQLStatement(StatementType::DELETE_STATEMENT) {
}

DeleteStatement::DeleteStatement(const DeleteStatement &other) : SQLStatement(other), table(other.table->Copy()) {
	if (other.condition) {
		condition = other.condition->Copy();
	}
	for (const auto &using_clause : other.using_clauses) {
		using_clauses.push_back(using_clause->Copy());
	}
	cte_map = other.cte_map.Copy();
}

string DeleteStatement::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "DELETE FROM ";
	result += table->ToString();
	if (!using_clauses.empty()) {
		result += " USING ";
		for (idx_t i = 0; i < using_clauses.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += using_clauses[i]->ToString();
		}
	}
	if (condition) {
		result += " WHERE " + condition->ToString();
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

unique_ptr<SQLStatement> DeleteStatement::Copy() const {
	return unique_ptr<DeleteStatement>(new DeleteStatement(*this));
}

unique_ptr<ParsedExpression> condition;
unique_ptr<TableRef> table;
vector<unique_ptr<TableRef>> using_clauses;
vector<unique_ptr<ParsedExpression>> returning_list;
CommonTableExpressionMap cte_map;

bool DeleteStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const DeleteStatement &)*other_p;

	if (!condition && !other.condition) {
		// both dont have a condition
	} else if (!condition || !other.condition) {
		// one of them has a condition, other doesn't
		return false;
	} else if (!condition->Equals(other.condition.get())) {
		// both have a condition, but they are not the same
		return false;
	}

	if (!table && !other.table) {

	} else if (!table || !other.table) {
		return false;
	} else if (!table->Equals(other.table.get()) {
		return false;
	}


	return true;
}

} // namespace duckdb
