//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/parsed_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/verification/statement_verifier.hpp"

namespace duckdb {

class ParsedStatementVerifier : public StatementVerifier {
public:
	explicit ParsedStatementVerifier(unique_ptr<SQLStatement> statement_p);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement);

	bool RequireEquality() const override {
		return false;
	}
};

} // namespace duckdb
