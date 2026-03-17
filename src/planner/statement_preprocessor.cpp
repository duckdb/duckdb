#include "duckdb/planner/statement_preprocessor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/parsed_data/bound_pragma_info.hpp"
#include "duckdb/function/function.hpp"

#include "duckdb/main/client_context.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/common/enums/active_transaction_state.hpp"

namespace duckdb {

enum class PreprocessingTransactionHandling : uint8_t {
	// Not in a transaction and should wrap in an implicit BEGIN/COMMIT
	WRAP_IN_TRANSACTION,
	// Already in an active transaction — set invalidation policy for multi-statement bodies
	SET_INVALIDATION_POLICY,
	// No transaction handling needed (single statement, or no transaction context applies)
	NONE
};

void AddStatements(vector<unique_ptr<SQLStatement>> &body_statements,
                   const PreprocessingTransactionHandling transaction_handling,
                   vector<unique_ptr<SQLStatement>> &result_statements) {
	if (transaction_handling == PreprocessingTransactionHandling::WRAP_IN_TRANSACTION) {
		auto begin_info = make_uniq<TransactionInfo>(
		    TransactionType::BEGIN_TRANSACTION, TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION, true);
		result_statements.push_back(make_uniq<TransactionStatement>(std::move(begin_info)));
	} else if (transaction_handling == PreprocessingTransactionHandling::SET_INVALIDATION_POLICY) {
		result_statements.push_back(make_uniq<SetVariableStatement>(
		    "current_transaction_invalidation_policy",
		    make_uniq<ConstantExpression>(Value("ALL_ERRORS_INVALIDATE_TRANSACTION")), SetScope::GLOBAL));
	}
	// insert body_statements into result_statements
	result_statements.insert(result_statements.end(), std::make_move_iterator(body_statements.begin()),
	                         std::make_move_iterator(body_statements.end()));
	if (transaction_handling == PreprocessingTransactionHandling::WRAP_IN_TRANSACTION) {
		auto commit_info = make_uniq<TransactionInfo>(
		    TransactionType::COMMIT, TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION, true);
		result_statements.push_back(make_uniq<TransactionStatement>(std::move(commit_info)));
	} else if (transaction_handling == PreprocessingTransactionHandling::SET_INVALIDATION_POLICY) {
		result_statements.push_back(
		    make_uniq<SetVariableStatement>("current_transaction_invalidation_policy",
		                                    make_uniq<ConstantExpression>(Value("STANDARD_POLICY")), SetScope::GLOBAL));
	}
}

StatementPreprocessor::StatementPreprocessor(ClientContext &context) : context(context) {
}

PreprocessingTransactionHandling GetTransactionHandling(vector<unique_ptr<SQLStatement>> &body_statements,
                                                        ActiveTransactionState full_transaction_state,
                                                        bool can_wrap = true) {
	if (body_statements.size() <= 1) {
		return PreprocessingTransactionHandling::NONE;
	}
	if (full_transaction_state == ActiveTransactionState::NO_OTHER_TRANSACTIONS && can_wrap) {
		return PreprocessingTransactionHandling::WRAP_IN_TRANSACTION;
	}
	if (full_transaction_state == ActiveTransactionState::OTHER_TRANSACTIONS) {
		return PreprocessingTransactionHandling::SET_INVALIDATION_POLICY;
	}
	return PreprocessingTransactionHandling::NONE;
}

void UnpackMultiStatement(MultiStatement &multi_statement, const ActiveTransactionState active_transaction_state,
                          vector<unique_ptr<SQLStatement>> &new_statements) {
#ifdef DEBUG // MultiStatement should not contain transaction statements
	for (auto &sub_statement : multi_statement.statements) {
		D_ASSERT(sub_statement->type != StatementType::TRANSACTION_STATEMENT);
	}
#endif
	bool has_select = false;
	for (auto &stmt : multi_statement.statements) {
		if (stmt->type == StatementType::SELECT_STATEMENT) {
			// Pivot statements have select, and we don't want to wrap those in transactions.
			has_select = true;
		}
	}
	bool can_wrap_in_transaction = !has_select;
	auto handling =
	    GetTransactionHandling(multi_statement.statements, active_transaction_state, can_wrap_in_transaction);
	AddStatements(multi_statement.statements, handling, new_statements);
}

vector<unique_ptr<SQLStatement>> StatementPreprocessor::TryReparsePragma(unique_ptr<SQLStatement> statement) const {
	// Try reparsing
	const auto info = statement->Cast<PragmaStatement>().info->Copy();
	QueryErrorContext error_context(statement->stmt_location);
	const auto binder = Binder::CreateBinder(context);
	const auto bound_info = binder->BindPragma(*info, error_context);
	if (bound_info->function.query) {
		// Needs reparsing
		FunctionParameters parameters {bound_info->parameters, bound_info->named_parameters};
		const auto query_to_reparse = bound_info->function.query(context, parameters);
		Parser parser(context.GetParserOptions());
		parser.ParseQuery(query_to_reparse);
		return std::move(parser.statements);
	}
	vector<unique_ptr<SQLStatement>> res;
	res.push_back(std::move(statement));
	return res;
}

void StatementPreprocessor::Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
                                       ActiveTransactionState transaction_context_state) {
	// Quick check: do we need preprocessing at all?
	bool needs_preprocessing = false;
	for (auto &stmt : statements) {
		if (stmt->type == StatementType::PRAGMA_STATEMENT || stmt->type == StatementType::MULTI_STATEMENT) {
			needs_preprocessing = true;
			break;
		}
	}
	if (!needs_preprocessing) {
		return;
	}

	context.RunFunctionInTransactionInternal(lock,
	                                         [&] { PreprocessInternal(lock, statements, transaction_context_state); });
}

void StatementPreprocessor::PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
                                               const ActiveTransactionState transaction_context_state) {
	ActiveTransactionState local_transaction_state = ActiveTransactionState::NO_OTHER_TRANSACTIONS;
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto query = statements[i]->query;
		const ActiveTransactionState full_transaction_state =
		    (transaction_context_state == ActiveTransactionState::OTHER_TRANSACTIONS ||
		     local_transaction_state == ActiveTransactionState::OTHER_TRANSACTIONS)
		        ? ActiveTransactionState::OTHER_TRANSACTIONS
		        : ActiveTransactionState::NO_OTHER_TRANSACTIONS;

		switch (statements[i]->type) {
		case StatementType::PRAGMA_STATEMENT: {
			auto reparsed_statements = TryReparsePragma(std::move(statements[i]));
			const auto handling = GetTransactionHandling(reparsed_statements, full_transaction_state);
			AddStatements(reparsed_statements, handling, new_statements);
			break;
		}
		case StatementType::MULTI_STATEMENT: {
			auto &multi_statement = statements[i]->Cast<MultiStatement>();
			UnpackMultiStatement(multi_statement, full_transaction_state, new_statements);
			break;
		}
		case StatementType::TRANSACTION_STATEMENT: {
			auto &transaction_stmt = statements[i]->Cast<TransactionStatement>();

			if (transaction_stmt.info->type == TransactionType::BEGIN_TRANSACTION) {
				new_statements.push_back(std::move(statements[i]));
				local_transaction_state = ActiveTransactionState::OTHER_TRANSACTIONS;
				break;
			}
			if (transaction_stmt.info->type == TransactionType::COMMIT ||
			    transaction_stmt.info->type == TransactionType::ROLLBACK) {
				local_transaction_state = ActiveTransactionState::NO_OTHER_TRANSACTIONS;
			}
			new_statements.push_back(std::move(statements[i]));
			break;
		}
		default: {
			new_statements.push_back(std::move(statements[i]));
		}
		}
	}

	statements = std::move(new_statements);
}
} // namespace duckdb
