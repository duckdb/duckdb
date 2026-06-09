//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parser_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parser_options.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/peg/token_type.hpp"

namespace duckdb {
struct DBConfig;

//! A minimal token view handed to parser extensions: the token text together with its classified
//! TokenType.
struct SimpleToken {
	SimpleToken(string text_p, TokenType type_p) : text(std::move(text_p)), type(type_p) {
	}

	//! The raw token text as it appears in the query
	string text;
	//! The classified type of the token
	TokenType type;
};

//! The ParserExtensionInfo holds static information relevant to the parser extension
//! It is made available in the parse_function, and will be kept alive as long as the database system is kept alive
struct ParserExtensionInfo {
	virtual ~ParserExtensionInfo() {
	}

	template <class TARGET>
	TARGET &Cast() {
		auto result = dynamic_cast<TARGET *>(this);
		if (!result) {
			throw InternalException("Failed to cast ParserExtensionInfo to type");
		}
		return *result;
	}
	template <class TARGET>
	const TARGET &Cast() const {
		auto result = dynamic_cast<const TARGET *>(this);
		if (!result) {
			throw InternalException("Failed to cast ParserExtensionInfo to type");
		}
		return *result;
	}
};

//===--------------------------------------------------------------------===//
// Parse
//===--------------------------------------------------------------------===//
enum class ParserExtensionResultType : uint8_t { PARSE_SUCCESSFUL, DISPLAY_ORIGINAL_ERROR, DISPLAY_EXTENSION_ERROR };

//! The ParserExtensionParseData holds the result of a successful parse step
//! It will be passed along to the subsequent plan function
struct ParserExtensionParseData {
	virtual ~ParserExtensionParseData() {
	}

	virtual unique_ptr<ParserExtensionParseData> Copy() const = 0;
	virtual string ToString() const = 0;
};

struct ParserExtensionParseResult {
	ParserExtensionParseResult() : type(ParserExtensionResultType::DISPLAY_ORIGINAL_ERROR) {
	}
	explicit ParserExtensionParseResult(string error_p)
	    : type(ParserExtensionResultType::DISPLAY_EXTENSION_ERROR), error(std::move(error_p)) {
	}
	explicit ParserExtensionParseResult(unique_ptr<ParserExtensionParseData> parse_data_p)
	    : type(ParserExtensionResultType::PARSE_SUCCESSFUL), parse_data(std::move(parse_data_p)) {
	}

	//! Whether or not parsing was successful
	ParserExtensionResultType type;
	//! The parse data (if successful)
	unique_ptr<ParserExtensionParseData> parse_data;
	//! The error message (if unsuccessful)
	string error;
	//! The error location (if unsuccessful)
	optional_idx error_location;
	//! How many leading tokens (from the start of the `tokens` view) the extension claimed:
	//!    > 0 : SUCCESS — the extension accepted that many tokens; the peeler resumes parsing
	//!          after them.
	//!   == 0 : the extension ran fine but did not claim any tokens (it's not taking this input) —
	//!          the peeler lets the next extension try.
	//!    < 0 : the extension wants to surface an error — the peeler throws (using `error` /
	//!          `error_location` if set).
	//! A positive count must not exceed the number of tokens in the view, or the peeler throws.
	int64_t consumed_tokens = 0;
};

//! Called when the PEG parser fails to parse a statement.
//!
//! `tokens` is the tokenized view of the source tail from the PEG failure point onward — one
//! SimpleToken (text + TokenType) per token, in source order. The extension dispatches on this
//! token stream and reports, via `ParserExtensionParseResult::consumed_tokens`, how many leading
//! tokens it claimed (> 0 success, 0 = ran but claimed nothing, < 0 = throw).
typedef ParserExtensionParseResult (*parse_function_t)(ParserExtensionInfo *info, const vector<SimpleToken> &tokens);
//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
struct ParserExtensionPlanResult { // NOLINT: work-around bug in clang-tidy
	//! The table function to execute
	TableFunction function;
	//! Parameters to the function
	vector<Value> parameters;
	//! The set of databases that will be modified by this statement (empty for a read-only statement)
	identifier_map_t<StatementProperties::ModificationInfo> modified_databases;
	//! Whether or not the statement requires a valid transaction to be executed
	bool requires_valid_transaction = true;
	//! What type of result set the statement returns
	StatementReturnType return_type = StatementReturnType::NOTHING;
};

typedef ParserExtensionPlanResult (*plan_function_t)(ParserExtensionInfo *info, ClientContext &context,
                                                     unique_ptr<ParserExtensionParseData> parse_data);

//===--------------------------------------------------------------------===//
// Parser override
//===--------------------------------------------------------------------===//
struct ParserOverrideResult {
	explicit ParserOverrideResult() : type(ParserExtensionResultType::DISPLAY_ORIGINAL_ERROR) {};

	explicit ParserOverrideResult(vector<unique_ptr<SQLStatement>> statements_p)
	    : type(ParserExtensionResultType::PARSE_SUCCESSFUL), statements(std::move(statements_p)) {};

	explicit ParserOverrideResult(std::exception &error_p)
	    : type(ParserExtensionResultType::DISPLAY_EXTENSION_ERROR), error(error_p) {};

	ParserExtensionResultType type;
	vector<unique_ptr<SQLStatement>> statements;
	ErrorData error;
};

typedef ParserOverrideResult (*parser_override_function_t)(ParserExtensionInfo *info, const string &query,
                                                           ParserOptions &options);

//===--------------------------------------------------------------------===//
// ParserExtension
//===--------------------------------------------------------------------===//
class ParserExtension {
public:
	//! The parse function of the parser extension.
	//! Takes a query string as input and returns ParserExtensionParseData (on success) or an error
	parse_function_t parse_function = nullptr;

	//! The plan function of the parser extension
	//! Takes as input the result of the parse_function, and outputs various properties of the resulting plan
	plan_function_t plan_function = nullptr;

	//! Override the current parser with a new parser and return a vector of SQL statements
	parser_override_function_t parser_override = nullptr;

	//! Additional parser info passed to the parse function
	shared_ptr<ParserExtensionInfo> parser_info;

	static void Register(DBConfig &config, ParserExtension extension);
};

} // namespace duckdb
