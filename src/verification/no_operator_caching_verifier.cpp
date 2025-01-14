#include "duckdb/verification/no_operator_caching_verifier.hpp"

namespace duckdb {

NoOperatorCachingVerifier::NoOperatorCachingVerifier(
    unique_ptr<SQLStatement> statement_p, optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters)
    : StatementVerifier(VerificationType::NO_OPERATOR_CACHING, "No operator caching", std::move(statement_p),
                        parameters) {
}

unique_ptr<StatementVerifier>
NoOperatorCachingVerifier::Create(const SQLStatement &statement_p,
                                  optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters) {
	return make_uniq<NoOperatorCachingVerifier>(statement_p.Copy(), parameters);
}

} // namespace duckdb
