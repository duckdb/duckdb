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

namespace duckdb {
class ClientContext;
class ClientContextLock;
class SQLStatement;
struct PragmaInfo;

//! Preprocesses parsed statements: expands pragmas, unpacks multi-statements, and wraps in transactions
class StatementPreprocessor {
public:
	explicit StatementPreprocessor(ClientContext &context);

	void Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
	                bool is_in_active_transaction);
	void PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
	                        bool is_in_active_transaction);

private:
	ClientContext &context;

private:
	//! Handles a pragma statement, determines whether the statement needs reparsing, if it does the 'resulting_query'
	//! contains the statement(s) to replace the current one
	void PragmaNeedsReparsing(SQLStatement &statement, string &resulting_query, bool &expanded) const;

	vector<unique_ptr<SQLStatement>> ReParse(const string &new_query) const;
};
} // namespace duckdb
