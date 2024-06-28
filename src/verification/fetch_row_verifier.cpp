#include "duckdb/verification/fetch_row_verifier.hpp"

namespace duckdb {

FetchRowVerifier::FetchRowVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::FETCH_ROW_AS_SCAN, "FetchRow as Scan", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> FetchRowVerifier::Create(const SQLStatement &statement_p) {
	return make_uniq<FetchRowVerifier>(statement_p.Copy());
}

} // namespace duckdb
