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

static void WrapInTransaction(vector<unique_ptr<SQLStatement>> &new_statements,
                              const std::function<void()> &emit_body) {
	auto begin_info = make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION,
	                                             TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION);
	new_statements.push_back(make_uniq<TransactionStatement>(std::move(begin_info)));

	emit_body();

	auto commit_info = make_uniq<TransactionInfo>(TransactionType::COMMIT,
	                                              TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION);
	new_statements.push_back(make_uniq<TransactionStatement>(std::move(commit_info)));
}

StatementPreprocessor::StatementPreprocessor(ClientContext &context) : context(context) {
}

void StatementPreprocessor::ExpandPragma(vector<unique_ptr<SQLStatement>> &new_statements, const string &new_query) {
	// this PRAGMA statement gets replaced by a new query string
	// push the new query string through the parser again and add it to the transformer
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(new_query);
	// insert the new statements and remove the old statement
	for (idx_t j = 0; j < parser.statements.size(); j++) {
		new_statements.push_back(std::move(parser.statements[j]));
	};
}

void UnpackMultiStatement(unique_ptr<MultiStatement> &multi_statement, bool is_in_active_transaction,
                          vector<unique_ptr<SQLStatement>> &new_statements) {
#ifdef DEBUG // MultiStatement should not contain transaction statements
	for (auto &sub_statement : multi_statement->statements) {
		D_ASSERT(sub_statement->type != StatementType::TRANSACTION_STATEMENT);
	}
#endif
	// if first sub-statement is SELECT, it is a PIVOT multistatement,
	// so we skip transaction wrapping to not break the PIVOT
	if (is_in_active_transaction || multi_statement->statements[0]->type == StatementType::SELECT_STATEMENT) {
		// add sub-statements
		for (auto &stmt : multi_statement->statements) {
			new_statements.push_back(std::move(stmt));
		}
	} else {
		WrapInTransaction(new_statements, [&] {
			for (auto &stmt : multi_statement->statements) {
				new_statements.push_back(std::move(stmt));
			}
		});
	}
}

void StatementPreprocessor::Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
                                       bool is_in_active_transaction) {
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		switch (statements[i]->type) {
		case StatementType::PRAGMA_STATEMENT: {
			string new_query;
			bool expanded;
			context.RunFunctionInTransactionInternal(lock,
			                                         [&] { TryExpandPragma(*statements[i], new_query, expanded); });
			if (!expanded || is_in_active_transaction) {
				new_statements.push_back(std::move(statements[i]));
				break;
			}
			WrapInTransaction(new_statements, [&] {
				context.RunFunctionInTransactionInternal(lock, [&] { ExpandPragma(new_statements, new_query); });
			});
			break;
		}
		case StatementType::MULTI_STATEMENT: {
			auto multi_statement = unique_ptr<MultiStatement>(static_cast<MultiStatement *>(statements[i].release()));
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

void StatementPreprocessor::TryExpandPragma(SQLStatement &statement, string &resulting_query, bool &expanded) {
	auto info = statement.Cast<PragmaStatement>().info->Copy();
	QueryErrorContext error_context(statement.stmt_location);
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindPragma(*info, error_context);
	if (bound_info->function.query) {
		FunctionParameters parameters {bound_info->parameters, bound_info->named_parameters};
		resulting_query = bound_info->function.query(context, parameters);
		expanded = true;
		return;
	}
	expanded = false;
}

} // namespace duckdb
