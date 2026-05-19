//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/connect_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/connect_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class ConnectStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::CONNECT_STATEMENT;

public:
	ConnectStatement();

	unique_ptr<ConnectInfo> info;

protected:
	ConnectStatement(const ConnectStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
