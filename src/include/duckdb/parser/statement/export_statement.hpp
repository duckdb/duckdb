//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/export_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {

class ExportStatement : public SQLStatement {
public:
	ExportStatement(unique_ptr<CopyInfo> info) : SQLStatement(StatementType::EXPORT_STATEMENT), info(move(info)) {};

	unique_ptr<CopyInfo> info;
};

} // namespace duckdb
