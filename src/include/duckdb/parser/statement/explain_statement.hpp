//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/explain_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/enums/explain_format.hpp"

namespace duckdb {

enum class ExplainType : uint8_t { EXPLAIN_STANDARD, EXPLAIN_ANALYZE };

class ExplainStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXPLAIN_STATEMENT;

public:
	explicit ExplainStatement(unique_ptr<SQLStatement> stmt, ExplainType explain_type = ExplainType::EXPLAIN_STANDARD,
	                          ExplainFormat explain_format = ExplainFormat::DEFAULT);

	unique_ptr<SQLStatement> stmt;
	ExplainType explain_type;
	ExplainFormat explain_format = ExplainFormat::DEFAULT;

protected:
	ExplainStatement(const ExplainStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string OptionsToString() const;
	string ToString() const override;
};

} // namespace duckdb
