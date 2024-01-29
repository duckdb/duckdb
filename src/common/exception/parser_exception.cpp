#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

ParserException::ParserException(const string &msg) : Exception(ExceptionType::PARSER, msg) {
}

ParserException ParserException::SyntaxError(const string &query, const string &error_message,
                                             optional_idx error_location) {
	ParserException result(error_location.IsValid()
	                           ? QueryErrorContext::Format(query, error_message, error_location.GetIndex())
	                           : error_message);
	result.InitializeExtraInfo("SYNTAX_ERROR", error_location);
	return result;
}

} // namespace duckdb
