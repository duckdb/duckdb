//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/disconnect_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/disconnect_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class DisconnectStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::DISCONNECT_STATEMENT;

public:
	DisconnectStatement();

	unique_ptr<DisconnectInfo> info;

protected:
	DisconnectStatement(const DisconnectStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
