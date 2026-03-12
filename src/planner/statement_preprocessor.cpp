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

namespace duckdb {

void AddStatements(vector<unique_ptr<SQLStatement>> &body_statements, bool should_wrap_in_transaction,
                   vector<unique_ptr<SQLStatement>> &result_statements) {
	if (should_wrap_in_transaction) {
		auto begin_info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION,
		                                             TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION);
		result_statements.push_back(make_uniq<TransactionStatement>(std::move(begin_info)));
	}
	// insert body_statements into result_statements
	result_statements.insert(result_statements.end(), std::make_move_iterator(body_statements.begin()),
	                         std::make_move_iterator(body_statements.end()));
	if (should_wrap_in_transaction) {
		auto commit_info = make_uniq<TransactionInfo>(TransactionType::COMMIT,
		                                              TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION);
		result_statements.push_back(make_uniq<TransactionStatement>(std::move(commit_info)));
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
	bool is_pivot_statement = false;
	for (auto &stmt : multi_statement.statements) {
		if (stmt->type == StatementType::SELECT_STATEMENT) {
			is_pivot_statement = true;
		}
	}
	AddStatements(multi_statement.statements, !is_pivot_statement && !is_in_active_transaction, new_statements);
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
                                       bool is_in_active_transaction) {
	// Quick check: do we need preprocessing at all?
	bool needs_preprocessing = false;
	for (auto &stmt : statements) {
		if (stmt->type == StatementType::PRAGMA_STATEMENT || stmt->type == StatementType::MULTI_STATEMENT) {
			needs_preprocessing = true;
			break;
		}
	}
	if (!needs_preprocessing)
		return;

	context.RunFunctionInTransactionInternal(lock,
	                                         [&] { PreprocessInternal(lock, statements, is_in_active_transaction); });
}

void StatementPreprocessor::PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
                                               bool is_in_active_transaction) {
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		switch (statements[i]->type) {
		case StatementType::PRAGMA_STATEMENT: {
			// string new_query;
			// bool needs_reparsing;
			// PragmaNeedsReparsing(, new_query, needs_reparsing);
			// if (needs_reparsing) {
			vector<unique_ptr<SQLStatement>> reparsed_statements = TryReparsePragma(std::move(statements[i]));
			AddStatements(reparsed_statements, !is_in_active_transaction && reparsed_statements.size() != 1,
			              new_statements);
			break;
			// }
			// new_statements.push_back(std::move(statements[i]));
			// break;
		}
		case StatementType::MULTI_STATEMENT: {
			auto &multi_statement = statements[i]->Cast<MultiStatement>();
			UnpackMultiStatement(multi_statement, is_in_active_transaction, new_statements);
			break;
		}
		case StatementType::TRANSACTION_STATEMENT: {
			const auto transaction_stmt = static_cast<TransactionStatement *>(statements[i].get());
			if (transaction_stmt->info->type == TransactionType::BEGIN_TRANSACTION) {
				is_in_active_transaction = true;
			} else if (transaction_stmt->info->type == TransactionType::COMMIT ||
			           transaction_stmt->info->type == TransactionType::ROLLBACK) {
				is_in_active_transaction = false;
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
