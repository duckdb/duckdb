//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/load_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class LoadStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::LOAD_STATEMENT;

public:
	LoadStatement();

protected:
	LoadStatement(const LoadStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;

	unique_ptr<LoadInfo> info;
};
} // namespace duckdb
