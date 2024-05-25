//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parser_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! The ParserExtensionInfo holds static information relevant to the parser extension
//! It is made available in the parse_function, and will be kept alive as long as the database system is kept alive
struct ParserExtensionInfo {
	virtual ~ParserExtensionInfo() {
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
};

typedef ParserExtensionParseResult (*parse_function_t)(ParserExtensionInfo *info, const string &query);
//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
struct ParserExtensionPlanResult { // NOLINT: work-around bug in clang-tidy
	//! The table function to execute
	TableFunction function;
	//! Parameters to the function
	vector<Value> parameters;
	//! The set of databases that will be modified by this statement (empty for a read-only statement)
	unordered_set<string> modified_databases;
	//! Whether or not the statement requires a valid transaction to be executed
	bool requires_valid_transaction = true;
	//! What type of result set the statement returns
	StatementReturnType return_type = StatementReturnType::NOTHING;
};

typedef ParserExtensionPlanResult (*plan_function_t)(ParserExtensionInfo *info, ClientContext &context,
                                                     unique_ptr<ParserExtensionParseData> parse_data);

//===--------------------------------------------------------------------===//
// ParserExtension
//===--------------------------------------------------------------------===//
class ParserExtension {
public:
	//! The parse function of the parser extension.
	//! Takes a query string as input and returns ParserExtensionParseData (on success) or an error
	parse_function_t parse_function;

	//! The plan function of the parser extension
	//! Takes as input the result of the parse_function, and outputs various properties of the resulting plan
	plan_function_t plan_function;

	//! Additional parser info passed to the parse function
	shared_ptr<ParserExtensionInfo> parser_info;
};

} // namespace duckdb
