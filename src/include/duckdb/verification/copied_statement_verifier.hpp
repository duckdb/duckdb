//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/copied_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/verification/statement_verifier.hpp"

namespace duckdb {

class CopiedStatementVerifier : public StatementVerifier {
public:
	explicit CopiedStatementVerifier(unique_ptr<SQLStatement> statement_p);

	static StatementVerifier Create(const SQLStatement &statement_p);
};

} // namespace duckdb
