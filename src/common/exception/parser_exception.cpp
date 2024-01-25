#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

ParserException::ParserException(const string &msg) : StandardException(ExceptionType::PARSER, msg) {
}

ParserException ParserException::SyntaxError(const string &query, const string &error_message, optional_idx error_location) {
	ParserException result(error_location.IsValid() ? QueryErrorContext::Format(query, error_message, error_location.GetIndex()) : error_message);
	if (error_location.IsValid()) {
		result.extra_info["position"] = to_string(error_location.GetIndex());
	}
	return result;
}

}
