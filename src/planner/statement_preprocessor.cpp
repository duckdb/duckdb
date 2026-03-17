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

namespace duckdb {

void AddStatements(vector<unique_ptr<SQLStatement>> &body_statements, bool should_wrap_in_transaction,
                   vector<unique_ptr<SQLStatement>> &result_statements, bool is_in_active_transaction) {
	if (should_wrap_in_transaction) {
		auto begin_info = make_uniq<TransactionInfo>(
		    TransactionType::BEGIN_TRANSACTION, TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION, true);
		result_statements.push_back(make_uniq<TransactionStatement>(std::move(begin_info)));
	} else if (is_in_active_transaction && body_statements.size() > 1) {
		result_statements.push_back(make_uniq<SetVariableStatement>(
		    "current_transaction_invalidation_policy",
		    make_uniq<ConstantExpression>(Value("ALL_ERRORS_INVALIDATE_TRANSACTION")), SetScope::GLOBAL));
	}
	// insert body_statements into result_statements
	result_statements.insert(result_statements.end(), std::make_move_iterator(body_statements.begin()),
	                         std::make_move_iterator(body_statements.end()));
	if (should_wrap_in_transaction) {
		auto commit_info = make_uniq<TransactionInfo>(
		    TransactionType::COMMIT, TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION, true);
		result_statements.push_back(make_uniq<TransactionStatement>(std::move(commit_info)));
	} else if (is_in_active_transaction && body_statements.size() > 1) {
		result_statements.push_back(
		    make_uniq<SetVariableStatement>("current_transaction_invalidation_policy",
		                                    make_uniq<ConstantExpression>(Value("STANDARD_POLICY")), SetScope::GLOBAL));
	}
}

StatementPreprocessor::StatementPreprocessor(ClientContext &context) : context(context) {
}

void UnpackMultiStatement(MultiStatement &multi_statement, bool is_in_active_transaction,
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
	AddStatements(multi_statement.statements, !has_select && !is_in_active_transaction, new_statements,
	              is_in_active_transaction);
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
                                       optional_ptr<TransactionContext> transaction_context) {
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

	context.RunFunctionInTransactionInternal(lock, [&] { PreprocessInternal(lock, statements, transaction_context); });
}

void StatementPreprocessor::PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
                                               optional_ptr<TransactionContext> transaction_context) {
	optional_ptr<TransactionStatement> chained_transaction = nullptr;
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto query = statements[i]->query;
		switch (statements[i]->type) {
		case StatementType::PRAGMA_STATEMENT: {
			auto reparsed_statements = TryReparsePragma(std::move(statements[i]));

			AddStatements(reparsed_statements,
			              !(transaction_context || chained_transaction) && reparsed_statements.size() != 1,
			              new_statements, transaction_context || chained_transaction);
			break;
		}
		case StatementType::MULTI_STATEMENT: {
			auto &multi_statement = statements[i]->Cast<MultiStatement>();
			UnpackMultiStatement(multi_statement, transaction_context || chained_transaction, new_statements);
			break;
		}
		case StatementType::TRANSACTION_STATEMENT: {
			auto &transaction_stmt = statements[i]->Cast<TransactionStatement>();

			if (transaction_stmt.info->type == TransactionType::BEGIN_TRANSACTION) {
				new_statements.push_back(std::move(statements[i]));
				chained_transaction = new_statements.back()->Cast<TransactionStatement>();
				break;
			}
			if (transaction_stmt.info->type == TransactionType::COMMIT ||
			    transaction_stmt.info->type == TransactionType::ROLLBACK) {
				chained_transaction = nullptr;
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
