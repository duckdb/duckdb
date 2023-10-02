#include "duckdb/parser/statement/relation_statement.hpp"

namespace duckdb {

RelationStatement::RelationStatement(shared_ptr<Relation> relation)
    : SQLStatement(StatementType::RELATION_STATEMENT), relation(std::move(relation)) {
}

unique_ptr<SQLStatement> RelationStatement::Copy() const {
	return unique_ptr<RelationStatement>(new RelationStatement(*this));
}

} // namespace duckdb
