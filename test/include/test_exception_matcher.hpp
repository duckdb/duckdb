#pragma once

#include "catch.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {

template <class MESSAGE_MATCHER>
class ExceptionMatcher : public Catch::MatcherBase<std::exception> {
public:
	ExceptionMatcher(ExceptionType type_p, MESSAGE_MATCHER message_p) : type(type_p), message(std::move(message_p)) {
	}

	bool match(const std::exception &exception) const override {
		const ErrorData error(exception);
		actual = Exception::ExceptionTypeToString(error.Type()) + ": " + error.RawMessage();
		this->m_cachedToString.clear();
		return error.Type() == type && message.match(error.RawMessage());
	}

	std::string describe() const override {
		auto description = Exception::ExceptionTypeToString(type) + " with raw message " + message.toString();
		if (!actual.empty()) {
			description += "\nactual: " + actual;
		}
		return description;
	}

private:
	ExceptionType type;
	MESSAGE_MATCHER message;
	mutable string actual;
};

template <class MESSAGE_MATCHER>
ExceptionMatcher<MESSAGE_MATCHER> ExceptionMatches(ExceptionType type, MESSAGE_MATCHER message) {
	return ExceptionMatcher<MESSAGE_MATCHER>(type, std::move(message));
}

} // namespace duckdb
