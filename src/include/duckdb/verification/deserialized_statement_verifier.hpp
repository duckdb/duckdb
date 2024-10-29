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
	explicit DeserializedStatementVerifier(unique_ptr<SQLStatement> statement_p,
	                                       optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement,
	                                            optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
};

} // namespace duckdb
