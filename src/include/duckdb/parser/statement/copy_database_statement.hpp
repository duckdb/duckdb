//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/copy_database_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class CopyDatabaseStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::COPY_DATABASE_STATEMENT;

public:
	CopyDatabaseStatement(string from_database, string to_database);

	string from_database;
	string to_database;

	string ToString() const override;

protected:
	CopyDatabaseStatement(const CopyDatabaseStatement &other);

public:
	DUCKDB_API unique_ptr<SQLStatement> Copy() const override;

private:
};
} // namespace duckdb
