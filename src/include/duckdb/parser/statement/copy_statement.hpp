//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class CopyStatement : public SQLStatement {
public:
	CopyStatement();

	unique_ptr<CopyInfo> info;
	// The SQL statement used instead of a table when copying data out to a file
	unique_ptr<QueryNode> select_statement;

protected:
	CopyStatement(const CopyStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace duckdb
