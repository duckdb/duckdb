//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/alter_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class AlterStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::ALTER_STATEMENT;

public:
	AlterStatement();

	unique_ptr<AlterInfo> info;

protected:
	AlterStatement(const AlterStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
