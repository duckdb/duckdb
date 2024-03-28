//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception/http_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

class HTTPException : public Exception {
public:
	template <typename>
	struct ResponseShape {
		typedef int status; // NOLINT
	};

	explicit HTTPException(const string &message) : Exception(ExceptionType::HTTP, message) {
	}

	template <class RESPONSE, typename ResponseShape<decltype(RESPONSE::status)>::status = 0, typename... ARGS>
	explicit HTTPException(RESPONSE &response, const string &msg, ARGS... params)
	    : HTTPException(response.status, response.body, response.headers, response.reason, msg, params...) {
	}

	template <typename>
	struct ResponseWrapperShape {
		typedef int code; // NOLINT
	};

	template <class RESPONSE, typename ResponseWrapperShape<decltype(RESPONSE::code)>::code = 0, typename... ARGS>
	explicit HTTPException(RESPONSE &response, const string &msg, ARGS... params)
	    : HTTPException(response.code, response.body, response.headers, response.error, msg, params...) {
	}

	template <class HEADERS, typename... ARGS>
	explicit HTTPException(int status_code, const string &response_body, const HEADERS &headers, const string &reason,
	                       const string &msg, ARGS... params)
	    : Exception(ExceptionType::HTTP, ConstructMessage(msg, params...),
	                HTTPExtraInfo(status_code, response_body, headers, reason)) {
	}

	template <class HEADERS>
	static unordered_map<string, string> HTTPExtraInfo(int status_code, const string &response_body,
	                                                   const HEADERS &headers, const string &reason) {
		unordered_map<string, string> extra_info;
		extra_info["status_code"] = to_string(status_code);
		extra_info["reason"] = reason;
		extra_info["response_body"] = response_body;
		for (auto &entry : headers) {
			extra_info["header_" + entry.first] = entry.second;
		}
		return extra_info;
	}
};

} // namespace duckdb
