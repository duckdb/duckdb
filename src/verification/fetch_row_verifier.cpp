#include "duckdb/verification/fetch_row_verifier.hpp"

namespace duckdb {

FetchRowVerifier::FetchRowVerifier(unique_ptr<SQLStatement> statement_p,
                                   optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters)
    : StatementVerifier(VerificationType::FETCH_ROW_AS_SCAN, "FetchRow as Scan", std::move(statement_p), parameters) {
}

unique_ptr<StatementVerifier>
FetchRowVerifier::Create(const SQLStatement &statement_p,
                         optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters) {
	return make_uniq<FetchRowVerifier>(statement_p.Copy(), parameters);
}

} // namespace duckdb
