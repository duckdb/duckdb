//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/explain_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

enum class ExplainType : uint8_t { EXPLAIN_STANDARD, EXPLAIN_ANALYZE };

class ExplainStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXPLAIN_STATEMENT;

public:
	explicit ExplainStatement(unique_ptr<SQLStatement> stmt, ExplainType explain_type = ExplainType::EXPLAIN_STANDARD);

	unique_ptr<SQLStatement> stmt;
	ExplainType explain_type;

protected:
	ExplainStatement(const ExplainStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
