//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/relation_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

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
