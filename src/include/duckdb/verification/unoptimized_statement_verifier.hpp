//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/unoptimized_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/verification/statement_verifier.hpp"

namespace duckdb {

class UnoptimizedStatementVerifier : public StatementVerifier {
public:
	explicit UnoptimizedStatementVerifier(unique_ptr<SQLStatement> statement_p);
	static StatementVerifier Create(const SQLStatement &statement_p);
	bool DisableOptimizer() const override {
		return true;
	}
};

} // namespace duckdb
