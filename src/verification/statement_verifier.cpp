#include "duckdb/verification/statement_verifier.hpp"

#include "duckdb/verification/copied_statement_verifier.hpp"
#include "duckdb/verification/deserialized_statement_verifier.hpp"
#include "duckdb/verification/external_statement_verifier.hpp"
#include "duckdb/verification/parsed_statement_verifier.hpp"
#include "duckdb/verification/prepared_statement_verifier.hpp"
#include "duckdb/verification/unoptimized_statement_verifier.hpp"

namespace duckdb {

StatementVerifier::StatementVerifier(string name, unique_ptr<SQLStatement> statement_p)
    : name(move(name)), statement(unique_ptr_cast<SQLStatement, SelectStatement>(move(statement_p))),
      select_list(statement->node->GetSelectList()) {
}

StatementVerifier StatementVerifier::Create(VerificationType type, const SQLStatement &statement_p) {
	switch (type) {
	case VerificationType::COPIED:
		return CopiedStatementVerifier::Create(statement_p);
	case VerificationType::DESERIALIZED:
		return DeserializedStatementVerifier::Create(statement_p);
	case VerificationType::PARSED:
		return ParsedStatementVerifier::Create(statement_p);
	case VerificationType::UNOPTIMIZED:
		return UnoptimizedStatementVerifier::Create(statement_p);
	case VerificationType::PREPARED:
		return PreparedStatementVerifier::Create(statement_p);
	case VerificationType::EXTERNAL:
		return ExternalStatementVerifier::Create(statement_p);
	case VerificationType::INVALID:
	default:
		throw InternalException("Invalid statement verification type!");
	}
}

} // namespace duckdb
