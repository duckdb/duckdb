//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/deserialized_statement_verifier_v2.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/verification/statement_verifier.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// This is a temporary statement verifier that uses the new de/serialization
// infrastructure to verify the correctness of the de/serialization process.
// This verifier will be removed once the new de/serialization infrastructure
// (FormatDe/Serializer) replaces the old one.
class DeserializedStatementVerifierV2 : public StatementVerifier {
public:
	explicit DeserializedStatementVerifierV2(unique_ptr<SQLStatement> statement_p);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement);
};

class DeserializedStatementVerifierNoDefaultV2 : public StatementVerifier {
public:
	explicit DeserializedStatementVerifierNoDefaultV2(unique_ptr<SQLStatement> statement_p);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement);
};

} // namespace duckdb
