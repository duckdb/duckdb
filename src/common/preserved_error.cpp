#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

PreservedError::PreservedError() : initialized(false), exception_instance(nullptr) {
}

PreservedError::PreservedError(const Exception &exception)
    : initialized(true), type(exception.type), raw_message(SanitizeErrorMessage(exception.RawMessage())),
      exception_instance(exception.Copy()) {
}

PreservedError::PreservedError(const string &message)
    : initialized(true), type(ExceptionType::INVALID), raw_message(SanitizeErrorMessage(message)),
      exception_instance(nullptr) {
	// Given a message in the form: 	xxxxx Error: yyyyy
	// Try to match xxxxxxx with known error so to potentially reconstruct the original error type
	auto position_semicolon = raw_message.find(':');
	if (position_semicolon == std::string::npos) {
		// Semicolon not found, bail out
		return;
	}
	if (position_semicolon + 2 >= raw_message.size()) {
		// Not enough characters afterward, bail out
		return;
	}
	string err = raw_message.substr(0, position_semicolon);
	string msg = raw_message.substr(position_semicolon + 2);
	if (err.size() > 6 && err.substr(err.size() - 6) == " Error" && !msg.empty()) {
		ExceptionType new_type = Exception::StringToExceptionType(err.substr(0, err.size() - 6));
		if (new_type != type) {
			type = new_type;
			raw_message = msg;
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
		Exception::ThrowAsTypeWithMessage(type, new_message, exception_instance);
	}
	Exception::ThrowAsTypeWithMessage(type, raw_message, exception_instance);
}

const ExceptionType &PreservedError::Type() const {
	D_ASSERT(initialized);
	return this->type;
}

PreservedError &PreservedError::AddToMessage(const string &prepended_message) {
	raw_message = prepended_message + raw_message;
	return *this;
}

PreservedError::operator bool() const {
	return initialized;
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

} // namespace duckdb
