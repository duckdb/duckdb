//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/multi_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class MultiStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::MULTI_STATEMENT;

public:
	MultiStatement();

	vector<unique_ptr<SQLStatement>> statements;

protected:
	MultiStatement(const MultiStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
