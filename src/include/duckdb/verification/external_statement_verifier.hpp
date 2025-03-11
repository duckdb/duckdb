//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/external_statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/verification/statement_verifier.hpp"

namespace duckdb {

class ExternalStatementVerifier : public StatementVerifier {
public:
	explicit ExternalStatementVerifier(unique_ptr<SQLStatement> statement_p,
	                                   optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement,
	                                            optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);

	bool ForceExternal() const override {
		return true;
	}
};

} // namespace duckdb
