//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/export_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {

class ExportStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXPORT_STATEMENT;

public:
	explicit ExportStatement(unique_ptr<CopyInfo> info);

	unique_ptr<CopyInfo> info;
	string database;

protected:
	ExportStatement(const ExportStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
