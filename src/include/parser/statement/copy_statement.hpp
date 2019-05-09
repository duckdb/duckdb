//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/copy_info.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

class CopyStatement : public SQLStatement {
public:
	CopyStatement() : SQLStatement(StatementType::COPY), info(make_unique<CopyInfo>()){};

	unique_ptr<CopyInfo> info;
	// The SQL statement used instead of a table when copying data out to a file
	unique_ptr<QueryNode> select_statement;
};
} // namespace duckdb
