//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception/http_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"

namespace duckdb {

class HTTPException : public IOException {
public:
	template <typename>
	struct ResponseShape {
		typedef int status;
	};

	template <class RESPONSE, typename ResponseShape<decltype(RESPONSE::status)>::status = 0, typename... ARGS>
	explicit HTTPException(RESPONSE &response, const string &msg, ARGS... params)
			: HTTPException(response.status, response.body, response.headers, response.reason, msg, params...) {
	}

	template <typename>
	struct ResponseWrapperShape {
		typedef int code;
	};
	template <class RESPONSE, typename ResponseWrapperShape<decltype(RESPONSE::code)>::code = 0, typename... ARGS>
	explicit HTTPException(RESPONSE &response, const string &msg, ARGS... params)
			: HTTPException(response.code, response.body, response.headers, response.error, msg, params...) {
	}

	template <typename HEADERS, typename... ARGS>
	explicit HTTPException(int status_code, string response_body, HEADERS headers, const string &reason,
						   const string &msg, ARGS... params)
			: IOException(ExceptionType::HTTP, ConstructMessage(msg, params...)), status_code(status_code), reason(reason),
			  response_body(std::move(response_body)) {
		this->headers.insert(headers.begin(), headers.end());
		D_ASSERT(this->headers.size() > 0);
	}

	std::shared_ptr<Exception> Copy() const {
		return make_shared<HTTPException>(status_code, response_body, headers, reason, RawMessage());
	}

	const std::multimap<std::string, std::string> GetHeaders() const {
		return headers;
	}
	int GetStatusCode() const {
		return status_code;
	}
	const string &GetResponseBody() const {
		return response_body;
	}
	const string &GetReason() const {
		return reason;
	}
	[[noreturn]] void Throw() const {
		throw HTTPException(status_code, response_body, headers, reason, RawMessage());
	}

private:
	int status_code;
	string reason;
	string response_body;
	std::multimap<string, string> headers;
};

} // namespace duckdb
