#include "duckdb/common/preserved_error.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/verification/statement_verifier.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

PreservedError ClientContext::VerifyQuery(ClientContextLock &lock, const string &query,
                                          unique_ptr<SQLStatement> statement) {
	D_ASSERT(statement->type == StatementType::SELECT_STATEMENT);
	// Aggressive query verification

	// The purpose of this function is to test correctness of otherwise hard to test features:
	// Copy() of statements and expressions
	// Serialize()/Deserialize() of expressions
	// Hash() of expressions
	// Equality() of statements and expressions
	// ToString() of statements and expressions
	// Correctness of plans both with and without optimizers

	const auto &stmt = *statement;
	vector<unique_ptr<StatementVerifier>> statement_verifiers;
	unique_ptr<StatementVerifier> prepared_statement_verifier;
	if (config.query_verification_enabled) {
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::COPIED, stmt));
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::DESERIALIZED, stmt));
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::PARSED, stmt));
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::UNOPTIMIZED, stmt));
		prepared_statement_verifier = StatementVerifier::Create(VerificationType::PREPARED, stmt);
	}
	if (config.verify_external) {
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::EXTERNAL, stmt));
	}

	auto original = make_unique<StatementVerifier>(move(statement));
	for (auto &verifier : statement_verifiers) {
		original->CheckExpressions(*verifier);
	}
	original->CheckExpressions();

	// See below
	auto statement_copy_for_explain = stmt.Copy();

	// Save settings
	bool optimizer_enabled = config.enable_optimizer;
	bool profiling_is_enabled = config.enable_profiler;
	bool force_external = config.force_external;

	// Disable profiling if it is enabled
	if (profiling_is_enabled) {
		config.enable_profiler = false;
	}

	// Execute the original statement
	bool any_failed = original->Run(*this, query, [&](const string &q, unique_ptr<SQLStatement> s) {
		return RunStatementInternal(lock, q, move(s), false, false);
	});
	// Execute the verifiers
	for (auto &verifier : statement_verifiers) {
		bool failed = verifier->Run(*this, query, [&](const string &q, unique_ptr<SQLStatement> s) {
			return RunStatementInternal(lock, q, move(s), false, false);
		});
		any_failed = any_failed || failed;
	}

	if (!any_failed && prepared_statement_verifier) {
		// If none failed, we execute the prepared statement verifier
		bool failed = prepared_statement_verifier->Run(*this, query, [&](const string &q, unique_ptr<SQLStatement> s) {
			return RunStatementInternal(lock, q, move(s), false, false);
		});
		if (!failed) {
			// PreparedStatementVerifier fails if it runs into a ParameterNotAllowedException, which is OK
			statement_verifiers.push_back(move(prepared_statement_verifier));
		}
	} else {
		if (db->IsInvalidated()) {
			return original->materialized_result->GetErrorObject();
		}
	}

	// Restore config setting
	config.enable_optimizer = optimizer_enabled;
	config.force_external = force_external;

	// Check explain, only if q does not already contain EXPLAIN
	if (original->materialized_result->success) {
		auto explain_q = "EXPLAIN " + query;
		auto explain_stmt = make_unique<ExplainStatement>(move(statement_copy_for_explain));
		try {
			RunStatementInternal(lock, explain_q, move(explain_stmt), false, false);
		} catch (std::exception &ex) { // LCOV_EXCL_START
			interrupted = false;
			return PreservedError("EXPLAIN failed but query did not (" + string(ex.what()) + ")");
		} // LCOV_EXCL_STOP

		// test the box renderer on the result
		// we mostly care that this does not crash
		RandomEngine random;
		BoxRendererConfig config;
		// test with a random width
		config.max_width = random.NextRandomInteger() % 500;
		BoxRenderer renderer(config);
		renderer.ToString(*this, original->materialized_result->names, original->materialized_result->Collection());
	}

	// Restore profiler setting
	if (profiling_is_enabled) {
		config.enable_profiler = true;
	}

	// Now compare the results
	// The results of all runs should be identical
	for (auto &verifier : statement_verifiers) {
		auto result = original->CompareResults(*verifier);
		if (!result.empty()) {
			return PreservedError(result);
		}
	}

	return PreservedError();
}

} // namespace duckdb
