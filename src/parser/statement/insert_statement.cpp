#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"

namespace duckdb {

InsertStatement::InsertStatement() : SQLStatement(StatementType::INSERT_STATEMENT), node(make_uniq<InsertQueryNode>()) {
}

InsertStatement::InsertStatement(const InsertStatement &other)
    : SQLStatement(other), node(unique_ptr_cast<QueryNode, InsertQueryNode>(other.node->Copy())) {
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
