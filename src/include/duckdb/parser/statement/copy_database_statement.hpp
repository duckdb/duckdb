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

enum class CopyDatabaseType { COPY_SCHEMA, COPY_DATA };

class CopyDatabaseStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::COPY_DATABASE_STATEMENT;

public:
	CopyDatabaseStatement(string from_database, string to_database, CopyDatabaseType copy_type);

	string from_database;
	string to_database;
	CopyDatabaseType copy_type;

protected:
	CopyDatabaseStatement(const CopyDatabaseStatement &other);

public:
	DUCKDB_API unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;

private:
};
} // namespace duckdb
