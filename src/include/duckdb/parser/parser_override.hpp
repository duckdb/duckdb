#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class SQLStatement;
class ClientContext;

enum class ParserOverrideOptions {
	THROW_ON_ERROR,
	CONTINUE_ON_ERROR,
	LOG_AND_CONTINUE_ON_ERROR
};

//! The ParserOverride is an extension point that allows installing a custom parser
class ParserOverride {
public:
	explicit ParserOverride(ParserOverrideOptions option_p) {
		option = option_p;
	};
	virtual ~ParserOverride() = default;

	//! Tries to parse a query.
	//! If it succeeds, it returns a vector of SQL statements.
	//! If it fails to parse because the syntax is not supported by this override,
	//! it should return an empty vector, allowing the system to fall back to the default parser.
	//! If it encounters a syntax error within a structure it *does* recognize, it should throw an exception.
	virtual vector<unique_ptr<SQLStatement>> Parse(const string &query) = 0;
	ParserOverrideOptions option = ParserOverrideOptions::THROW_ON_ERROR;
};

} // namespace duckdb
