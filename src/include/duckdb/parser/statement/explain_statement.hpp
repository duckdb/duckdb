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

enum class ExplainType : uint8_t {
	EXPLAIN_STANDARD,
	EXPLAIN_ANALYZE
};

class ExplainStatement : public SQLStatement {
public:
	ExplainStatement(unique_ptr<SQLStatement> stmt, ExplainType explain_type = ExplainType::EXPLAIN_STANDARD);

	unique_ptr<SQLStatement> stmt;
	ExplainType explain_type;

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
