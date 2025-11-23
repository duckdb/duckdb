#include "duckdb/common/error_data.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/stacktrace.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

ErrorData::ErrorData() : initialized(false), type(ExceptionType::INVALID) {
}

ErrorData::ErrorData(const std::exception &ex) : ErrorData(ex.what()) {
}

ErrorData::ErrorData(ExceptionType type, const string &message)
    : initialized(true), type(type), raw_message(SanitizeErrorMessage(message)),
      final_message(ConstructFinalMessage()) {
}

ErrorData::ErrorData(const string &message)
    : initialized(true), type(ExceptionType::INVALID), raw_message(string()), final_message(string()) {
	// parse the constructed JSON
	if (message.empty() || message[0] != '{') {
		// not JSON! Use the message as a raw Exception message and leave type as uninitialized

		if (message == std::bad_alloc().what()) {
			type = ExceptionType::OUT_OF_MEMORY;
			raw_message = "Allocation failure";
		} else {
			raw_message = message;
		}
	} else {
		auto info = StringUtil::ParseJSONMap(message)->Flatten();
		for (auto &entry : info) {
			if (entry.first == "exception_type") {
				type = Exception::StringToExceptionType(entry.second);
			} else if (entry.first == "exception_message") {
				raw_message = SanitizeErrorMessage(entry.second);
			} else {
				extra_info[entry.first] = entry.second;
			}
		}
	}

	final_message = ConstructFinalMessage();
}

string ErrorData::SanitizeErrorMessage(string error) {
	return StringUtil::Replace(std::move(error), string("\0", 1), "\\0");
}

string ErrorData::ConstructFinalMessage() const {
	std::string error;
	if (type != ExceptionType::UNKNOWN_TYPE) {
		error = Exception::ExceptionTypeToString(type) + " ";
	}
	error += "Error: " + raw_message;
	if (type == ExceptionType::INTERNAL || type == ExceptionType::FATAL) {
		error += "\nThis error signals an assertion failure within DuckDB. This usually occurs due to "
		         "unexpected conditions or errors in the program's logic.\nFor more information, see "
		         "https://duckdb.org/docs/stable/dev/internal_errors";

		// Ensure that we print the stack trace for internal and fatal exceptions.
		auto entry = extra_info.find("stack_trace_pointers");
		if (entry != extra_info.end()) {
			auto stack_trace = StackTrace::ResolveStacktraceSymbols(entry->second);
			error += "\n\nStack Trace:\n" + stack_trace;
		}
	}
	return error;
}

void ErrorData::Throw(const string &prepended_message) const {
	D_ASSERT(initialized);
	if (!prepended_message.empty()) {
		string new_message = prepended_message + raw_message;
		throw Exception(extra_info, type, new_message);
	} else {
		throw Exception(extra_info, type, raw_message);
	}
}

const ExceptionType &ErrorData::Type() const {
	D_ASSERT(initialized);
	return this->type;
}

void ErrorData::Merge(const ErrorData &other) {
	if (!other.HasError()) {
		return;
	}
	if (!HasError()) {
		*this = other;
		return;
	}
	final_message += "\n\n" + other.Message();
}

bool ErrorData::operator==(const ErrorData &other) const {
	if (initialized != other.initialized) {
		return false;
	}
	if (type != other.type) {
		return false;
	}
	return raw_message == other.raw_message;
}

void ErrorData::ConvertErrorToJSON() {
	if (!raw_message.empty() && raw_message[0] == '{') {
		// empty or already JSON
		return;
	}
	raw_message = StringUtil::ExceptionToJSONMap(type, raw_message, extra_info);
	final_message = raw_message;
}

void ErrorData::FinalizeError() {
	auto entry = extra_info.find("stack_trace_pointers");
	if (entry != extra_info.end()) {
		auto stack_trace = StackTrace::ResolveStacktraceSymbols(entry->second);
		extra_info["stack_trace"] = std::move(stack_trace);
		extra_info.erase("stack_trace_pointers");
	}
}

void ErrorData::AddErrorLocation(const string &query) {
	if (!query.empty()) {
		auto entry = extra_info.find("position");
		if (entry != extra_info.end()) {
			raw_message = QueryErrorContext::Format(query, raw_message, std::stoull(entry->second));
		}
	}
	{
		auto entry = extra_info.find("stack_trace");
		if (entry != extra_info.end() && !entry->second.empty()) {
			raw_message += "\n\nStack Trace:\n" + entry->second;
			entry->second = "";
		}
	}
	final_message = ConstructFinalMessage();
}

void ErrorData::AddQueryLocation(optional_idx query_location) {
	Exception::SetQueryLocation(query_location, extra_info);
}

void ErrorData::AddQueryLocation(QueryErrorContext error_context) {
	AddQueryLocation(error_context.query_location);
}

void ErrorData::AddQueryLocation(const ParsedExpression &ref) {
	AddQueryLocation(ref.GetQueryLocation());
}

void ErrorData::AddQueryLocation(const TableRef &ref) {
	AddQueryLocation(ref.query_location);
}

} // namespace duckdb
