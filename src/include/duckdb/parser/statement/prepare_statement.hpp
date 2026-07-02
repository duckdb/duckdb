//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/prepare_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class PrepareStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::PREPARE_STATEMENT;

public:
	PrepareStatement();

	unique_ptr<SQLStatement> statement;
	string name;

protected:
	PrepareStatement(const PrepareStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};
} // namespace duckdb
