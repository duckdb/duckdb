//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/vacuum_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

class VacuumStatement : public SQLStatement {
public:
	VacuumStatement() : SQLStatement(StatementType::VACUUM_STATEMENT){};
	unique_ptr<VacuumInfo> info;
};

} // namespace duckdb
