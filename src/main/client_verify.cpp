#include "duckdb/common/error_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/verification/statement_verifier.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/enums/debug_statement_verification.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

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

void ClientContext::StatementVerification(unique_ptr<SQLStatement> &statement) {
	auto verification = Settings::Get<DebugVerifyStatementSetting>(*this);
	if (verification == DebugStatementVerification::COPY_STATEMENT) {
		if (statement->type == StatementType::LOGICAL_PLAN_STATEMENT) {
			// COPY verification not supported for plan statements
			return;
		}
		statement = statement->Copy();
	} else if (verification == DebugStatementVerification::REPARSE_STATEMENT) {
		if (statement->type == StatementType::RELATION_STATEMENT) {
			// reparsing not supported for relation statements
			return;
		}
		Parser parser(GetParserOptions());
		ErrorData error;
		parser.ParseQuery(statement->ToString());
		// FIXME: these properties don't round-trip in ToString(), so we overwrite them manually
		if (statement->type == StatementType::UPDATE_STATEMENT) {
			// re-apply `prioritize_table_when_binding` (which is normally set during transform)
			parser.statements[0]->Cast<UpdateStatement>().node->prioritize_table_when_binding =
			    statement->Cast<UpdateStatement>().node->prioritize_table_when_binding;
		} else if (statement->type == StatementType::TRANSACTION_STATEMENT) {
			// re-apply invalidation policy
			auto &reparsed_transaction_stmt = parser.statements[0]->Cast<TransactionStatement>();
			auto &previous_transaction_stmt = statement->Cast<TransactionStatement>();
			reparsed_transaction_stmt.info->invalidation_policy = previous_transaction_stmt.info->invalidation_policy;
			// re-apply auto rollback
			reparsed_transaction_stmt.info->auto_rollback = statement->Cast<TransactionStatement>().info->auto_rollback;
		}
		statement = std::move(parser.statements[0]);
	} else if (verification == DebugStatementVerification::SERIALIZE_STATEMENT) {
		switch (statement->type) {
		case StatementType::SELECT_STATEMENT:
		case StatementType::INSERT_STATEMENT:
		case StatementType::DELETE_STATEMENT:
		case StatementType::UPDATE_STATEMENT:
			break;
		default:
			// unsupported statement for serialization
			return;
		}
		Allocator allocator;
		MemoryStream stream(allocator);
		SerializationOptions options;
		options.serialization_compatibility = SerializationCompatibility::FromString("latest");
		optional_ptr<SelectStatement> to_serialize_stmt;
		optional_ptr<QueryNode> to_serialize_node;
		switch (statement->type) {
		case StatementType::SELECT_STATEMENT:
			to_serialize_stmt = statement->Cast<SelectStatement>();
			break;
		case StatementType::INSERT_STATEMENT:
			to_serialize_node = (QueryNode &)*statement->Cast<InsertStatement>().node;
			break;
		case StatementType::DELETE_STATEMENT:
			to_serialize_node = (QueryNode &)*statement->Cast<DeleteStatement>().node;
			break;
		case StatementType::UPDATE_STATEMENT:
			to_serialize_node = (QueryNode &)*statement->Cast<UpdateStatement>().node;
			break;
		default:
			throw InternalException("Unsupported type for serialization verification");
		}
		// do the round-trip
		unique_ptr<SelectStatement> deserialized_stmt;
		unique_ptr<QueryNode> deserialized_node;
		if (to_serialize_stmt) {
			BinarySerializer::Serialize(*to_serialize_stmt, stream, options);
			stream.Rewind();
			deserialized_stmt = BinaryDeserializer::Deserialize<SelectStatement>(stream);
		} else {
			BinarySerializer::Serialize(*to_serialize_node, stream, options);
			stream.Rewind();
			deserialized_node = BinaryDeserializer::Deserialize<QueryNode>(stream);
		}

		switch (statement->type) {
		case StatementType::SELECT_STATEMENT:
			statement = std::move(deserialized_stmt);
			break;
		case StatementType::INSERT_STATEMENT: {
			auto result = make_uniq<InsertStatement>();
			result->node = unique_ptr_cast<QueryNode, InsertQueryNode>(std::move(deserialized_node));
			statement = std::move(result);
			break;
		}
		case StatementType::DELETE_STATEMENT: {
			auto result = make_uniq<DeleteStatement>();
			result->node = unique_ptr_cast<QueryNode, DeleteQueryNode>(std::move(deserialized_node));
			statement = std::move(result);
			break;
		}
		case StatementType::UPDATE_STATEMENT: {
			auto result = make_uniq<UpdateStatement>();
			result->node = unique_ptr_cast<QueryNode, UpdateQueryNode>(std::move(deserialized_node));
			statement = std::move(result);
			break;
		}
		default:
			throw InternalException("Unsupported type for serialization verification");
		}
	}
}

ErrorData ClientContext::VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
                                     PendingQueryParameters query_parameters) {
	D_ASSERT(statement->type == StatementType::SELECT_STATEMENT);
	// Aggressive query verification

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
		// FIXME: Prepared parameter verifier is broken for queries with parameters
		if (!parameters || parameters->empty()) {
			prepared_statement_verifier = StatementVerifier::Create(VerificationType::PREPARED, stmt, parameters);
		}
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
		ClientBoxRendererContext render_context(*this);
		renderer.ToString(render_context, original->materialized_result->names, pinned_result_set->collection);
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
