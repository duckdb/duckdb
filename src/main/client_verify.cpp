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
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"

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

struct PreparedStatementVerification {
	PreparedStatementVerification() {
	}

	void ConvertConstants(QueryNode &node);
	void ConvertConstants(unique_ptr<ParsedExpression> &child);

	case_insensitive_map_t<unique_ptr<ParsedExpression>> values;
};

void PreparedStatementVerification::ConvertConstants(QueryNode &node) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
}

void PreparedStatementVerification::ConvertConstants(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		// constant: extract the constant value
		auto alias = expr->GetAlias();
		expr->ClearAlias();
		// check if the value already exists
		idx_t index = values.size();
		auto identifier = std::to_string(index + 1);
		const auto predicate = [&](const std::pair<const string, unique_ptr<ParsedExpression>> &pair) {
			return pair.second->Equals(*expr.get());
		};
		auto result = std::find_if(values.begin(), values.end(), predicate);
		if (result == values.end()) {
			// If it doesn't exist yet, add it
			values[identifier] = std::move(expr);
		} else {
			identifier = result->first;
		}

		// replace it with an expression
		auto parameter = make_uniq<ParameterExpression>();
		parameter->identifier = identifier;
		parameter->SetAlias(alias);
		expr = std::move(parameter);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*expr,
	                                            [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
}

void ClientContext::StatementVerification(ClientContextLock &lock, const string &query,
                                          unique_ptr<SQLStatement> &statement,
                                          PendingQueryParameters query_parameters) {
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
	} else if (verification == DebugStatementVerification::PREPARED_STATEMENT) {
		if (statement->type != StatementType::SELECT_STATEMENT) {
			// only supported for SELECT (for now?)
			return;
		}
		if (!statement->named_param_map.empty()) {
			// not supported for statements that already take parameters
			return;
		}
		if (query_parameters.parameters && !query_parameters.parameters->empty()) {
			// not supported for statements that already have parameters
			return;
		}
		auto &select_stmt = statement->Cast<SelectStatement>();
		auto node = select_stmt.node->Copy();

		// convert constants in the query node to parameters
		// i.e. turn SELECT 'hello', 42 into SELECT ?, ?
		PreparedStatementVerification prep_verifier;
		prep_verifier.ConvertConstants(*node);

		// create the prepared select
		// i.e. PREPARE p AS SELECT ?, ?
		auto prepare_base = make_uniq<SelectStatement>();
		prepare_base->node = std::move(node);
		for (auto &kv : prep_verifier.values) {
			prepare_base->named_param_map[kv.first] = 0;
		}

		// create the PREPARE and EXECUTE statements
		string name = "__duckdb_verification_prepared_statement_" + UUID::ToString(UUID::GenerateRandomUUID());
		auto prepare = make_uniq<PrepareStatement>();
		prepare->name = name;
		prepare->statement = std::move(prepare_base);

		// execute the PREPARE
		ErrorData error;
		try {
			auto prepare_result = RunStatementInternal(lock, string(), std::move(prepare), query_parameters);
			if (prepare_result->HasError()) {
				error = prepare_result->GetErrorObject();
			}
		} catch (std::exception &ex) {
			error = ErrorData(ex);
		}
		if (error.HasError()) {
			// FIXME: this is extremely lenient...
			if (error.Type() == ExceptionType::INTERNAL) {
				error.Throw("Failed prepare during verify: ");
			}
			return;
		}

		// create and return the EXECUTE statement
		// i.e. EXECUTE p('hello', 42)
		auto execute = make_uniq<ExecuteStatement>();
		execute->name = name;
		execute->named_values = std::move(prep_verifier.values);

		statement = std::move(execute);
	} else if (verification == DebugStatementVerification::EXPLAIN_STATEMENT) {
		if (statement->type == StatementType::EXPLAIN_STATEMENT) {
			// don't explain explain...
			return;
		}
		if (!statement->named_param_map.empty()) {
			// not supported for statements that already take parameters
			return;
		}
		if (query_parameters.parameters && !query_parameters.parameters->empty()) {
			// not supported for statements that already have parameters
			return;
		}
		auto explain_q = "EXPLAIN " + query;
		auto explain_stmt = make_uniq<ExplainStatement>(statement->Copy());
		auto explain_result = RunStatementInternal(lock, explain_q, std::move(explain_stmt), query_parameters);
		if (explain_result->HasError()) {
			explain_result->ThrowError();
		}
	}
}

} // namespace duckdb
