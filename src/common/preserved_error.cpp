#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

PreservedError::PreservedError() : initialized(false), type(ExceptionType::INVALID) {
}

PreservedError::PreservedError(const std::exception &ex) : PreservedError(ex.what()) {
}

PreservedError::PreservedError(ExceptionType type, const string &message)
    : initialized(true), type(type), raw_message(SanitizeErrorMessage(message)) {
}

PreservedError::PreservedError(const string &message)
    : initialized(true), type(ExceptionType::INVALID), raw_message(string()) {

	// parse the constructed JSON
	if (message.empty() || message[0] != '{') {
		// not JSON! Use the message as a raw Exception message and leave type as uninitialized
		raw_message = message;
		return;
	} else {
		auto info = StringUtil::ParseJSONMap(message);
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
}

const string &PreservedError::Message() {
	if (final_message.empty()) {
		final_message = Exception::ExceptionTypeToString(type) + " Error: " + raw_message;
	}
	return final_message;
}

string PreservedError::SanitizeErrorMessage(string error) {
	return StringUtil::Replace(std::move(error), string("\0", 1), "\\0");
}

void PreservedError::Throw(const string &prepended_message) const {
	D_ASSERT(initialized);
	if (!prepended_message.empty()) {
		string new_message = prepended_message + raw_message;
		throw Exception(type, new_message, extra_info);
	} else {
		throw Exception(type, raw_message, extra_info);
	}
}

const ExceptionType &PreservedError::Type() const {
	D_ASSERT(initialized);
	return this->type;
}

bool PreservedError::operator==(const PreservedError &other) const {
	if (initialized != other.initialized) {
		return false;
	}
	if (type != other.type) {
		return false;
	}
	return raw_message == other.raw_message;
}

void PreservedError::ConvertErrorToJSON() {
	if (raw_message.empty() || raw_message[0] == '{') {
		// empty or already JSON
		return;
	}
	raw_message = StringUtil::ToJSONMap(type, raw_message, extra_info);
	final_message = raw_message;
}

void PreservedError::AddErrorLocation(const string &query) {
	auto entry = extra_info.find("position");
	if (entry == extra_info.end()) {
		return;
	}
	raw_message = QueryErrorContext::Format(query, raw_message, std::stoull(entry->second));
}

void PreservedError::AddQueryLocation(optional_idx query_location) {
	Exception::SetQueryLocation(query_location, extra_info);
}

void PreservedError::AddQueryLocation(QueryErrorContext error_context) {
	AddQueryLocation(error_context.query_location);
}

void PreservedError::AddQueryLocation(const ParsedExpression &ref) {
	AddQueryLocation(ref.query_location);
}

void PreservedError::AddQueryLocation(const TableRef &ref) {
	AddQueryLocation(ref.query_location);
}

} // namespace duckdb
