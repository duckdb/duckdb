//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/export_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

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
