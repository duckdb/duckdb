#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class SQLStatement;
class ClientContext;

enum class OnParserOverrideError {
	THROW_ON_ERROR,
	CONTINUE_ON_ERROR
};

class ParserOverrideOptions {
public:
	ParserOverrideOptions(OnParserOverrideError error_p, bool enable_logging_p) : error(error_p), enable_logging(enable_logging_p) {
	}

	unique_ptr<ParserOverrideOptions> Copy() {
		return make_uniq<ParserOverrideOptions>(error, enable_logging);
	}

	OnParserOverrideError error;
	bool enable_logging;
};

//! The ParserOverride is an extension point that allows installing a custom parser
class ParserOverride {
public:
	explicit ParserOverride(unique_ptr<ParserOverrideOptions> options_p, ClientContext &context_p) : options(std::move(options_p)), context(context_p) {
	};
	virtual ~ParserOverride() = default;

	//! Tries to parse a query.
	//! If it succeeds, it returns a vector of SQL statements.
	//! If it fails to parse because the syntax is not supported by this override,
	//! it should return an empty vector, allowing the system to fall back to the default parser.
	//! If it encounters a syntax error within a structure it *does* recognize, it should throw an exception.
	virtual vector<unique_ptr<SQLStatement>> Parse(const string &query) = 0;
	unique_ptr<ParserOverrideOptions> options;
	ClientContext &context;

	bool LoggingEnabled() {
		return options->enable_logging;
	}

	bool ThrowOnError() {
		return options->error == OnParserOverrideError::THROW_ON_ERROR;
	}

	void LogQuery(const string &message);
	void LogError(const string &message, const std::exception &e);
};

} // namespace duckdb
