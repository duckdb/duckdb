//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

enum class VerificationType : uint8_t {
	ORIGINAL,
	COPIED,
	DESERIALIZED,
	PARSED,
	UNOPTIMIZED,
	NO_OPERATOR_CACHING,
	PREPARED,
	EXTERNAL,
	FETCH_ROW_AS_SCAN,

	INVALID
};

class StatementVerifier {
public:
	StatementVerifier(VerificationType type, string name, unique_ptr<SQLStatement> statement_p);
	explicit StatementVerifier(unique_ptr<SQLStatement> statement_p);
	static unique_ptr<StatementVerifier> Create(VerificationType type, const SQLStatement &statement_p);
	virtual ~StatementVerifier() noexcept;

	//! Check whether expressions in this verifier and the other verifier match
	void CheckExpressions(const StatementVerifier &other) const;
	//! Check whether expressions within this verifier match
	void CheckExpressions() const;

	//! Run the select statement and store the result
	virtual bool Run(ClientContext &context, const string &query,
	                 const std::function<unique_ptr<QueryResult>(const string &, unique_ptr<SQLStatement>)> &run);
	//! Compare this verifier's results with another verifier
	string CompareResults(const StatementVerifier &other);

public:
	const VerificationType type;
	const string name;
	unique_ptr<SelectStatement> statement;
	const vector<unique_ptr<ParsedExpression>> &select_list;
	unique_ptr<MaterializedQueryResult> materialized_result;

	virtual bool RequireEquality() const {
		return true;
	}

	virtual bool DisableOptimizer() const {
		return false;
	}

	virtual bool DisableOperatorCaching() const {
		return false;
	}

	virtual bool ForceExternal() const {
		return false;
	}

	virtual bool ForceFetchRow() const {
		return false;
	}
};

} // namespace duckdb
