//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/pragma_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"

namespace duckdb {

class PragmaStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::PRAGMA_STATEMENT;

public:
	PragmaStatement();

	unique_ptr<PragmaInfo> info;

protected:
	PragmaStatement(const PragmaStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
