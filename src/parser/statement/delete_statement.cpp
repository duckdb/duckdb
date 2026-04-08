#include "duckdb/parser/statement/delete_statement.hpp"

#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

DeleteStatement::DeleteStatement() : SQLStatement(StatementType::DELETE_STATEMENT), node(make_uniq<DeleteQueryNode>()) {
}

DeleteStatement::DeleteStatement(const DeleteStatement &other)
    : SQLStatement(other), node(unique_ptr_cast<QueryNode, DeleteQueryNode>(other.node->Copy())) {
}

string DeleteStatement::ToString() const {
	return node->ToString();
}

unique_ptr<SQLStatement> DeleteStatement::Copy() const {
	return unique_ptr<DeleteStatement>(new DeleteStatement(*this));
}

} // namespace duckdb
