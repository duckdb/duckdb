//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class DeleteQueryNode;

class DeleteStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::DELETE_STATEMENT;

public:
	DeleteStatement();

	unique_ptr<DeleteQueryNode> node;

protected:
	DeleteStatement(const DeleteStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
