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
#include "duckdb/main/client_context_lock.hpp"

namespace duckdb {
class ClientContext;
class SQLStatement;
struct PragmaInfo;
//! Preprocesses parsed statements: expands pragmas, unpacks multi-statements, and wraps in transactions
class StatementPreprocessor {
public:
	explicit StatementPreprocessor(ClientContext &context);
	void Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
	                CurrentTransactionState transaction_context_state) DUCKDB_REQUIRES(lock);
	void PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
	                        CurrentTransactionState transaction_context_state) DUCKDB_REQUIRES(lock);

private:
	ClientContext &context;

private:
	//! Handles a pragma statement, determines whether the statement needs reparsing, if it does, it returns the
	//! statement(s) to replace the current one. Otherwise, it just returns back the original statement in a vector.
	vector<unique_ptr<SQLStatement>> TryReparsePragma(unique_ptr<SQLStatement> statement) const;
};
} // namespace duckdb
