#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/main/relation/query_relation.hpp"

namespace duckdb {

RelationStatement::RelationStatement(shared_ptr<Relation> relation_p)
    : SQLStatement(StatementType::RELATION_STATEMENT), relation(std::move(relation_p)) {
	if (relation->type == RelationType::QUERY_RELATION) {
		auto &query_relation = relation->Cast<QueryRelation>();
		query = query_relation.query;
	}
}

unique_ptr<SQLStatement> RelationStatement::Copy() const {
	return unique_ptr<RelationStatement>(new RelationStatement(*this));
}

string RelationStatement::ToString() const {
	return relation->ToString();
}

} // namespace duckdb
