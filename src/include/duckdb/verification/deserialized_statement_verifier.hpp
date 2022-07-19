//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/deserialized_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/verification/statement_verifier.hpp"

namespace duckdb {

class DeserializedStatementVerifier : public StatementVerifier {
public:
	explicit DeserializedStatementVerifier(unique_ptr<SQLStatement> statement_p);

	static StatementVerifier Create(const SQLStatement &statement);
};

} // namespace duckdb
