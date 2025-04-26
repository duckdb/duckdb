//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/refresh_matview_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class RefreshMatViewStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::REFRESH_MATVIEW_STATEMENT;

public:
	RefreshMatViewStatement();

	unique_ptr<CreateInfo> info;

protected:
	RefreshMatViewStatement(const RefreshMatViewStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
