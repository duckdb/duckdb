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

namespace duckdb {
struct DBConfig;

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
	//! On PARSE_SUCCESSFUL, how many bytes of `query` the extension consumed (measured from the
	//! start of the view). The peeler resumes parsing from view-start + consumed_chars. A
	//! successful result with consumed_chars == 0 is treated as a decline (lets the next
	//! extension try).
	//!
	//! On success, `consumed_chars` MUST be one of the values in the `allowed_boundaries` vector
	//! passed to the parse function. Otherwise the peeler throws a ParserException.
	idx_t consumed_chars = 0;
};

//! Called when the PEG parser fails to parse a statement.
//!
//! `query` is the tail of the source from the PEG failure point onward.
//! `allowed_boundaries` is the set of valid stopping points inside `query` — the start AND end
//! offsets of every token, plus a final entry equal to `query.size()`. Sorted ascending,
//! deduplicated (adjacent tokens with no whitespace between them contribute a single shared
//! boundary). On success, the extension must report `consumed_chars` equal to one of these
//! values. Reporting 0 declines and lets the next extension or the original PEG error surface.
typedef ParserExtensionParseResult (*parse_function_t)(ParserExtensionInfo *info, const string &query,
                                                       const vector<idx_t> &allowed_boundaries);
//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
struct ParserExtensionPlanResult { // NOLINT: work-around bug in clang-tidy
	//! The table function to execute
	TableFunction function;
	//! Parameters to the function
	vector<Value> parameters;
	//! The set of databases that will be modified by this statement (empty for a read-only statement)
	unordered_map<string, StatementProperties::ModificationInfo> modified_databases;
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
