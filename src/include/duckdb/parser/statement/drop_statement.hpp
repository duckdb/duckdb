//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/drop_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class DropStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::DROP_STATEMENT;

public:
	DropStatement();

	unique_ptr<DropInfo> info;

protected:
	DropStatement(const DropStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
