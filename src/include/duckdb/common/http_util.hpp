//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/http_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {
class DatabaseInstance;
class HTTPLogger;

enum class HTTPStatusCode : uint16_t {
	INVALID = 0,
	// Information responses
	Continue_100 = 100,
	SwitchingProtocol_101 = 101,
	Processing_102 = 102,
	EarlyHints_103 = 103,

	// Successful responses
	OK_200 = 200,
	Created_201 = 201,
	Accepted_202 = 202,
	NonAuthoritativeInformation_203 = 203,
	NoContent_204 = 204,
	ResetContent_205 = 205,
	PartialContent_206 = 206,
	MultiStatus_207 = 207,
	AlreadyReported_208 = 208,
	IMUsed_226 = 226,

	// Redirection messages
	MultipleChoices_300 = 300,
	MovedPermanently_301 = 301,
	Found_302 = 302,
	SeeOther_303 = 303,
	NotModified_304 = 304,
	UseProxy_305 = 305,
	unused_306 = 306,
	TemporaryRedirect_307 = 307,
	PermanentRedirect_308 = 308,

	// Client error responses
	BadRequest_400 = 400,
	Unauthorized_401 = 401,
	PaymentRequired_402 = 402,
	Forbidden_403 = 403,
	NotFound_404 = 404,
	MethodNotAllowed_405 = 405,
	NotAcceptable_406 = 406,
	ProxyAuthenticationRequired_407 = 407,
	RequestTimeout_408 = 408,
	Conflict_409 = 409,
	Gone_410 = 410,
	LengthRequired_411 = 411,
	PreconditionFailed_412 = 412,
	PayloadTooLarge_413 = 413,
	UriTooLong_414 = 414,
	UnsupportedMediaType_415 = 415,
	RangeNotSatisfiable_416 = 416,
	ExpectationFailed_417 = 417,
	ImATeapot_418 = 418,
	MisdirectedRequest_421 = 421,
	UnprocessableContent_422 = 422,
	Locked_423 = 423,
	FailedDependency_424 = 424,
	TooEarly_425 = 425,
	UpgradeRequired_426 = 426,
	PreconditionRequired_428 = 428,
	TooManyRequests_429 = 429,
	RequestHeaderFieldsTooLarge_431 = 431,
	UnavailableForLegalReasons_451 = 451,

	// Server error responses
	InternalServerError_500 = 500,
	NotImplemented_501 = 501,
	BadGateway_502 = 502,
	ServiceUnavailable_503 = 503,
	GatewayTimeout_504 = 504,
	HttpVersionNotSupported_505 = 505,
	VariantAlsoNegotiates_506 = 506,
	InsufficientStorage_507 = 507,
	LoopDetected_508 = 508,
	NotExtended_510 = 510,
	NetworkAuthenticationRequired_511 = 511,
};

struct HTTPHeaders {
	using header_map_t = std::multimap<string, string>;

public:
	HTTPHeaders() = default;
	explicit HTTPHeaders(DatabaseInstance &db);

	void Insert(string key, string value);
	bool HasHeader(const string &key) const;
	string GetHeaderValue(const string &key) const;

	header_map_t::iterator begin() { // NOLINT: match stl API
		return headers.begin();
	}
	header_map_t::iterator end() { // NOLINT: match stl API
		return headers.end();
	}
	header_map_t::const_iterator begin() const { // NOLINT: match stl API
		return headers.begin();
	}
	header_map_t::const_iterator end() const { // NOLINT: match stl API
		return headers.end();
	}
	header_map_t::const_iterator cbegin() const { // NOLINT: match stl API
		return headers.begin();
	}
	header_map_t::const_iterator cend() const { // NOLINT: match stl API
		return headers.end();
	}

private:
	header_map_t headers;
};

struct HTTPResponse {
	explicit HTTPResponse(HTTPStatusCode code);

	HTTPStatusCode status;
	string body;
	string request_error;
	string reason;
	HTTPHeaders headers;
	bool success = true;

public:
	bool HasHeader(const string &key) const;
	string GetHeaderValue(const string &key) const;

	bool Success() const;

	bool HasRequestError() const;
	const string &GetRequestError() const;
};

class HTTPUtil {
public:
	virtual ~HTTPUtil() = default;

public:
	static HTTPUtil &Get(DatabaseInstance &db);

	virtual unique_ptr<HTTPResponse> Request(DatabaseInstance &db, const string &url, const HTTPHeaders &headers,
	                                         optional_ptr<HTTPLogger> http_logger);

	static void ParseHTTPProxyHost(string &proxy_value, string &hostname_out, idx_t &port_out, idx_t default_port = 80);
	static HTTPStatusCode ToStatusCode(int32_t status_code);
};
} // namespace duckdb
