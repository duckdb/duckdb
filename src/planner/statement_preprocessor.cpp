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
#include "duckdb/common/enums/current_transaction_state.hpp"

namespace duckdb {

void AddStatements(vector<unique_ptr<SQLStatement>> &body_statements,
                   const PreprocessingTransactionHandling transaction_handling,
                   vector<unique_ptr<SQLStatement>> &result_statements) {
	if (transaction_handling == PreprocessingTransactionHandling::WRAP_IN_TRANSACTION) {
		auto begin_info = make_uniq<TransactionInfo>(
		    TransactionType::BEGIN_TRANSACTION, TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION, true);
		auto begin_stmt = make_uniq<TransactionStatement>(std::move(begin_info));
		begin_stmt->query = begin_stmt->ToString();
		result_statements.push_back(std::move(begin_stmt));
	} else if (transaction_handling == PreprocessingTransactionHandling::SET_INVALIDATION_POLICY) {
		// Here we do a `SET current_transaction_invalidation_policy='ALL_ERRORS_INVALIDATE_TRANSACTION';`, for the
		// current transaction, to make sure multistatements/pragmas are fully transactional, and invalidate even with
		// minor errors such as binder, parser, etc.
		auto set_stmt = make_uniq<SetVariableStatement>("current_transaction_invalidation_policy",
		                                                ConstantExpression::String("ALL_ERRORS_INVALIDATE_TRANSACTION"),
		                                                SetScope::GLOBAL);
		set_stmt->query = set_stmt->ToString();
		result_statements.push_back(std::move(set_stmt));
	}

	// insert body_statements into result_statements
	result_statements.insert(result_statements.end(), std::make_move_iterator(body_statements.begin()),
	                         std::make_move_iterator(body_statements.end()));

	if (transaction_handling == PreprocessingTransactionHandling::WRAP_IN_TRANSACTION) {
		auto commit_info = make_uniq<TransactionInfo>(
		    TransactionType::COMMIT, TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION, true);
		auto commit_stmt = make_uniq<TransactionStatement>(std::move(commit_info));
		commit_stmt->query = commit_stmt->ToString();
		result_statements.push_back(std::move(commit_stmt));
	} else if (transaction_handling == PreprocessingTransactionHandling::SET_INVALIDATION_POLICY) {
		auto set_stmt = make_uniq<SetVariableStatement>(
		    "current_transaction_invalidation_policy", ConstantExpression::String("STANDARD_POLICY"), SetScope::GLOBAL);
		set_stmt->query = set_stmt->ToString();
		result_statements.push_back(std::move(set_stmt));
	}
}

StatementPreprocessor::StatementPreprocessor(ClientContext &context) : context(context) {
}

PreprocessingTransactionHandling GetTransactionHandling(vector<unique_ptr<SQLStatement>> &body_statements,
                                                        CurrentTransactionState full_transaction_state,
                                                        bool skip_handling = false) {
	if (body_statements.size() <= 1 || skip_handling) {
		return PreprocessingTransactionHandling::NONE;
	}
	if (full_transaction_state == NOT_IN_ACTIVE_TRANSACTION) {
		return PreprocessingTransactionHandling::WRAP_IN_TRANSACTION;
	}
	if (full_transaction_state == IN_ACTIVE_TRANSACTION) {
		return PreprocessingTransactionHandling::SET_INVALIDATION_POLICY;
	}
	return PreprocessingTransactionHandling::NONE;
}

PreprocessedStatement UnpackMultiStatement(MultiStatement &multi_statement,
                                           const CurrentTransactionState current_transaction_state) {
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
	PreprocessedStatement result;
	result.transaction_handling =
	    GetTransactionHandling(multi_statement.statements, current_transaction_state, has_select);
	result.statements = std::move(multi_statement.statements);
	return result;
}

vector<unique_ptr<SQLStatement>> StatementPreprocessor::TryReparsePragma(unique_ptr<SQLStatement> statement) const {
	// Try reparsing
	const auto info = statement->Cast<PragmaStatement>().info->Copy();
	QueryErrorContext error_context(statement->stmt_location);
	const auto binder = Binder::CreateBinder(context);
	const auto bound_info = binder->BindPragma(*info, error_context);
	if (bound_info->function.query) {
		// Needs reparsing
		FunctionParameters parameters {bound_info->parameters, bound_info->named_parameters, &bound_info->function};
		const auto query_to_reparse = bound_info->function.query(context, parameters);
		Parser parser(context);
		parser.ParseQuery(query_to_reparse);
		return std::move(parser.statements);
	}
	vector<unique_ptr<SQLStatement>> res;
	res.push_back(std::move(statement));
	return res;
}

void StatementPreprocessor::Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
                                       CurrentTransactionState transaction_context_state) {
	// Quick check: do we need preprocessing at all?
	bool needs_preprocessing = false;
	// Only PRAGMA and multi-statements touch the catalog and need a transaction.
	bool needs_transaction = false;
	for (auto &stmt : statements) {
		if (stmt->type == StatementType::PRAGMA_STATEMENT || stmt->type == StatementType::MULTI_STATEMENT) {
			needs_preprocessing = true;
			needs_transaction = true;
		} else if (stmt->type == StatementType::TRANSACTION_STATEMENT) {
			needs_preprocessing = true;
		}
	}
	if (!needs_preprocessing) {
		return;
	}
	if (!needs_transaction) {
		// Transaction statements only update chained_transaction_state. Wrapping them in a transaction would start a
		// throwaway autocommit transaction for a BEGIN (consuming a transaction id), and would make a ROLLBACK of an
		// aborted transaction fail.
		PreprocessInternal(lock, statements, transaction_context_state);
		return;
	}

	context.RunFunctionInTransactionInternal(lock,
	                                         [&] { PreprocessInternal(lock, statements, transaction_context_state); });
}

void StatementPreprocessor::PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
                                               const CurrentTransactionState transaction_context_state) {
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto query = statements[i]->query;
		const CurrentTransactionState full_transaction_state =
		    (transaction_context_state == IN_ACTIVE_TRANSACTION || chained_transaction_state == IN_ACTIVE_TRANSACTION)
		        ? IN_ACTIVE_TRANSACTION
		        : NOT_IN_ACTIVE_TRANSACTION;

		switch (statements[i]->type) {
		case StatementType::PRAGMA_STATEMENT:
		case StatementType::MULTI_STATEMENT: {
			auto expanded = ExpandStatement(std::move(statements[i]), full_transaction_state);
			AddStatements(expanded.statements, expanded.transaction_handling, new_statements);
			break;
		}
		case StatementType::TRANSACTION_STATEMENT: {
			auto &transaction_stmt = statements[i]->Cast<TransactionStatement>();

			if (transaction_stmt.info->type == TransactionType::BEGIN_TRANSACTION) {
				new_statements.push_back(std::move(statements[i]));
				chained_transaction_state = IN_ACTIVE_TRANSACTION;
				break;
			}
			if (transaction_stmt.info->type == TransactionType::COMMIT ||
			    transaction_stmt.info->type == TransactionType::ROLLBACK) {
				chained_transaction_state = NOT_IN_ACTIVE_TRANSACTION;
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

PreprocessedStatement StatementPreprocessor::ExpandStatement(unique_ptr<SQLStatement> statement,
                                                             CurrentTransactionState transaction_state) const {
	if (statement->type == StatementType::MULTI_STATEMENT) {
		return UnpackMultiStatement(statement->Cast<MultiStatement>(), transaction_state);
	}
	D_ASSERT(statement->type == StatementType::PRAGMA_STATEMENT);
	PreprocessedStatement result;
	result.statements = TryReparsePragma(std::move(statement));
	result.transaction_handling = GetTransactionHandling(result.statements, transaction_state);
	return result;
}

PreprocessedStatement StatementPreprocessor::PreprocessStatement(ClientContextLock &lock,
                                                                 unique_ptr<SQLStatement> statement,
                                                                 CurrentTransactionState transaction_context_state) {
	if (statement->type != StatementType::PRAGMA_STATEMENT && statement->type != StatementType::MULTI_STATEMENT) {
		PreprocessedStatement result;
		result.statements.push_back(std::move(statement));
		return result;
	}
	// Only PRAGMA and multi-statements touch the catalog and need a transaction
	PreprocessedStatement result;
	context.RunFunctionInTransactionInternal(
	    lock, [&] { result = ExpandStatement(std::move(statement), transaction_context_state); });
	if (result.statements.size() > 1) {
		for (auto &expanded : result.statements) {
			// The statements run as one query in one transaction, which a transaction statement would end or take over
			if (expanded->type == StatementType::TRANSACTION_STATEMENT) {
				throw InvalidInputException(
				    "A statement that expands into several cannot contain a transaction statement: %s",
				    expanded->query);
			}
		}
	}
	return result;
}
} // namespace duckdb
