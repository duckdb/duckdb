//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

enum class CopyToType : uint8_t { COPY_TO_FILE, EXPORT_DATABASE };

class CopyStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::COPY_STATEMENT;

public:
	CopyStatement();

	unique_ptr<CopyInfo> info;

	string ToString() const override;

protected:
	CopyStatement(const CopyStatement &other);

public:
	DUCKDB_API unique_ptr<SQLStatement> Copy() const override;

private:
};
} // namespace duckdb
