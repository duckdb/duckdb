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
	explicit RelationStatement(shared_ptr<Relation> relation);

	shared_ptr<Relation> relation;

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
