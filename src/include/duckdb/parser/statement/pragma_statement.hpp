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
	PragmaStatement() : SQLStatement(StatementType::PRAGMA_STATEMENT), info(make_unique<PragmaInfo>()){};

	unique_ptr<PragmaInfo> info;
};

} // namespace duckdb
