//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/alter_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"

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
