#include "duckdb/common/error_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/verification/statement_verifier.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

static void ThrowIfExceptionIsInternal(StatementVerifier &verifier) {
	if (!verifier.materialized_result) {
		return;
	}
	auto &result = *verifier.materialized_result;
	if (!result.HasError()) {
		return;
	}
	auto &error = result.GetErrorObject();
	if (error.Type() == ExceptionType::INTERNAL) {
		error.Throw();
	}
}

ErrorData ClientContext::VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
                                     PendingQueryParameters query_parameters) {
	D_ASSERT(statement->type == StatementType::SELECT_STATEMENT);
	// Aggressive query verification

#ifdef DUCKDB_RUN_SLOW_VERIFIERS
	bool run_slow_verifiers = true;
#else
	bool run_slow_verifiers = false;
#endif

	auto parameters = query_parameters.parameters;
	query_parameters.query_parameters.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	query_parameters.query_parameters.memory_type = QueryResultMemoryType::IN_MEMORY;

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

	// Base Statement verifiers: these are the verifiers we enable for regular builds
	if (config.query_verification_enabled) {
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::COPIED, stmt, parameters));
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::DESERIALIZED, stmt, parameters));
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::UNOPTIMIZED, stmt, parameters));

		// FIXME: Prepared parameter verifier is broken for queries with parameters
		if (!parameters || parameters->empty()) {
			prepared_statement_verifier = StatementVerifier::Create(VerificationType::PREPARED, stmt, parameters);
		}
	}

	// This verifier is enabled explicitly OR by enabling run_slow_verifiers
	if (config.verify_fetch_row || (run_slow_verifiers && config.query_verification_enabled)) {
		statement_verifiers.emplace_back(
		    StatementVerifier::Create(VerificationType::FETCH_ROW_AS_SCAN, stmt, parameters));
	}

	// For the DEBUG_ASYNC build we enable this extra verifier
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (config.query_verification_enabled) {
		statement_verifiers.emplace_back(
		    StatementVerifier::Create(VerificationType::NO_OPERATOR_CACHING, stmt, parameters));
	}
#endif

	// Verify external always needs to be explicitly enabled and is never part of default verifier set
	if (config.verify_external) {
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::EXTERNAL, stmt, parameters));
	}

	auto original = make_uniq<StatementVerifier>(std::move(statement), parameters);
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
	bool any_failed = original->Run(*this, query,
	                                [&](const string &q, unique_ptr<SQLStatement> s,
	                                    optional_ptr<case_insensitive_map_t<BoundParameterData>> params) {
		                                return RunStatementInternal(lock, q, std::move(s), query_parameters, false);
	                                });
	if (!any_failed) {
		statement_verifiers.emplace_back(
		    StatementVerifier::Create(VerificationType::PARSED, *statement_copy_for_explain, parameters));
	}
	// Execute the verifiers
	for (auto &verifier : statement_verifiers) {
		bool failed = verifier->Run(*this, query,
		                            [&](const string &q, unique_ptr<SQLStatement> s,
		                                optional_ptr<case_insensitive_map_t<BoundParameterData>> params) {
			                            return RunStatementInternal(lock, q, std::move(s), query_parameters, false);
		                            });
		any_failed = any_failed || failed;
	}

	if (!any_failed && prepared_statement_verifier) {
		// If none failed, we execute the prepared statement verifier
		bool failed = prepared_statement_verifier->Run(
		    *this, query,
		    [&](const string &q, unique_ptr<SQLStatement> s,
		        optional_ptr<case_insensitive_map_t<BoundParameterData>> params) {
			    return RunStatementInternal(lock, q, std::move(s), query_parameters, false);
		    });
		if (!failed) {
			// PreparedStatementVerifier fails if it runs into a ParameterNotAllowedException, which is OK
			statement_verifiers.push_back(std::move(prepared_statement_verifier));
		} else {
			// If it does fail, let's make sure it's not an internal exception
			ThrowIfExceptionIsInternal(*prepared_statement_verifier);
		}
	} else {
		if (ValidChecker::IsInvalidated(*db)) {
			return original->materialized_result->GetErrorObject();
		}
		if (transaction.HasActiveTransaction() && ValidChecker::IsInvalidated(ActiveTransaction())) {
			return original->materialized_result->GetErrorObject();
		}
	}

	// Restore config setting
	config.enable_optimizer = optimizer_enabled;
	config.force_external = force_external;

	// Check explain, only if q does not already contain EXPLAIN
	if (original->materialized_result->success) {
		auto explain_q = "EXPLAIN " + query;
		auto original_named_param_map = statement_copy_for_explain->named_param_map;
		auto explain_stmt = make_uniq<ExplainStatement>(std::move(statement_copy_for_explain));
		explain_stmt->named_param_map = original_named_param_map;

		auto explain_statement_verifier =
		    StatementVerifier::Create(VerificationType::EXPLAIN, *explain_stmt, parameters);
		const auto explain_failed = explain_statement_verifier->Run(
		    *this, explain_q,
		    [&](const string &q, unique_ptr<SQLStatement> s,
		        optional_ptr<case_insensitive_map_t<BoundParameterData>> params) {
			    return RunStatementInternal(lock, q, std::move(s), query_parameters, false);
		    });

		if (explain_failed) { // LCOV_EXCL_START
			const auto &explain_error = explain_statement_verifier->materialized_result->error;
			return ErrorData(explain_error.Type(), StringUtil::Format("Query succeeded but EXPLAIN failed with: %s",
			                                                          explain_error.RawMessage()));
		} // LCOV_EXCL_STOP

#ifdef DUCKDB_VERIFY_BOX_RENDERER
		// this is pretty slow, so disabled by default
		// test the box renderer on the result
		// we mostly care that this does not crash
		RandomEngine random;
		BoxRendererConfig config;
		// test with a random width
		config.max_width = random.NextRandomInteger() % 500;
		BoxRenderer renderer(config);
		auto pinned_result_set = original->materialized_result->Pin();
		renderer.ToString(*this, original->materialized_result->names, pinned_result_set->collection);
#endif
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
			return ErrorData(result);
		}
	}

	return ErrorData();
}

} // namespace duckdb
