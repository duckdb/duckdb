//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement_preprocessor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/common/enums/current_transaction_state.hpp"

namespace duckdb {
class ClientContext;
class ClientContextLock;
class SQLStatement;
struct PragmaInfo;

enum class PreprocessingTransactionHandling : uint8_t {
	// Not in a transaction and should wrap in an implicit BEGIN/COMMIT
	WRAP_IN_TRANSACTION,
	// Already in an active transaction: set invalidation policy for multi-statement bodies
	SET_INVALIDATION_POLICY,
	// No transaction handling needed (single statement, or no transaction context applies)
	NONE
};

//! The statements one user statement expands into, and the transaction handling they need
struct PreprocessedStatement {
	vector<unique_ptr<SQLStatement>> statements;
	PreprocessingTransactionHandling transaction_handling = PreprocessingTransactionHandling::NONE;
};

//! Preprocesses parsed statements: expands pragmas, unpacks multi-statements, and wraps in transactions
class StatementPreprocessor {
public:
	explicit StatementPreprocessor(ClientContext &context);
	void Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
	                CurrentTransactionState transaction_context_state);
	void PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
	                        CurrentTransactionState transaction_context_state);
	//! Expands one user statement without adding the statements of its transaction handling, which the caller
	//! applies itself
	PreprocessedStatement PreprocessStatement(ClientContextLock &lock, unique_ptr<SQLStatement> statement,
	                                          CurrentTransactionState transaction_context_state);

private:
	ClientContext &context;
	//! Transaction state implied by explicit transaction statements seen so far. Kept across Preprocess calls,
	//! because statements may all be preprocessed before any of them is executed.
	CurrentTransactionState chained_transaction_state = NOT_IN_ACTIVE_TRANSACTION;

private:
	//! Handles a pragma statement, determines whether the statement needs reparsing, if it does, it returns the
	//! statement(s) to replace the current one. Otherwise, it just returns back the original statement in a vector.
	vector<unique_ptr<SQLStatement>> TryReparsePragma(unique_ptr<SQLStatement> statement) const;
	//! Expands a pragma or multi-statement into the statements it stands for
	PreprocessedStatement ExpandStatement(unique_ptr<SQLStatement> statement,
	                                      CurrentTransactionState transaction_state) const;
};
} // namespace duckdb
