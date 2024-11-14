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

class FetchRowVerifier : public StatementVerifier {
public:
	explicit FetchRowVerifier(unique_ptr<SQLStatement> statement_p,
	                          optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);
	static unique_ptr<StatementVerifier> Create(const SQLStatement &statement_p,
	                                            optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters);

	bool ForceFetchRow() const override {
		return true;
	}
};

} // namespace duckdb
