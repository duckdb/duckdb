//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/copy_database_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"

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
