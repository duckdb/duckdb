//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/verification/statement_verifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

enum class VerificationType : uint8_t {
	COPIED = 0,
	DESERIALIZED = 1,
	PARSED = 2,
	UNOPTIMIZED = 3,
	PREPARED = 4,
	EXTERNAL = 5,

	INVALID = 255
};

class StatementVerifier {
public:
	StatementVerifier(string name, unique_ptr<SQLStatement> statement_p);

	static StatementVerifier Create(VerificationType type, const SQLStatement &statement_p);

	virtual bool RequireEquality() const {
		return true;
	}

	virtual bool DisableOptimizer() const {
		return false;
	}

	virtual bool ForceExternal() const {
		return false;
	}

public:
	const string name;
	unique_ptr<SelectStatement> statement;
	const vector<unique_ptr<ParsedExpression>> &select_list;
};

} // namespace duckdb
