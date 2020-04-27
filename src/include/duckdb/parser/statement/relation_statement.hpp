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
	RelationStatement(shared_ptr<Relation> relation) : SQLStatement(StatementType::RELATION_STATEMENT), relation(move(relation)) {
	}

	shared_ptr<Relation> relation;
};

} // namespace duckdb
