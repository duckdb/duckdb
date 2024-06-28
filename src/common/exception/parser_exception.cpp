#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

ParserException::ParserException(const string &msg) : Exception(ExceptionType::PARSER, msg) {
}

ParserException::ParserException(const string &msg, const unordered_map<string, string> &extra_info)
    : Exception(ExceptionType::PARSER, msg, extra_info) {
}

ParserException ParserException::SyntaxError(const string &query, const string &error_message,
                                             optional_idx error_location) {
	return ParserException(error_message, Exception::InitializeExtraInfo("SYNTAX_ERROR", error_location));
}

} // namespace duckdb
