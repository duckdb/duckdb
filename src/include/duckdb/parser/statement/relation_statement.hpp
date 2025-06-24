//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/relation_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/main/relation.hpp"

namespace duckdb {

class RelationStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::RELATION_STATEMENT;

public:
	explicit RelationStatement(shared_ptr<Relation> relation);

	shared_ptr<Relation> relation;

protected:
	RelationStatement(const RelationStatement &other) = default;

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
