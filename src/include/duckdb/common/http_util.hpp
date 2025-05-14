//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/http_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include <functional>

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

struct HTTPParams {
	virtual ~HTTPParams() = default;

	static constexpr uint64_t DEFAULT_TIMEOUT_SECONDS = 30; // 30 sec
	static constexpr uint64_t DEFAULT_RETRIES = 3;
	static constexpr uint64_t DEFAULT_RETRY_WAIT_MS = 100;
	static constexpr float DEFAULT_RETRY_BACKOFF = 4;
	static constexpr bool DEFAULT_KEEP_ALIVE = true;

	uint64_t timeout = DEFAULT_TIMEOUT_SECONDS; // seconds component of a timeout
	uint64_t timeout_usec = 0;                  // usec component of a timeout
	uint64_t retries = DEFAULT_RETRIES;
	uint64_t retry_wait_ms = DEFAULT_RETRY_WAIT_MS;
	float retry_backoff = DEFAULT_RETRY_BACKOFF;
	bool keep_alive = DEFAULT_KEEP_ALIVE;

	string http_proxy;
	idx_t http_proxy_port;
	string http_proxy_username;
	string http_proxy_password;
	unordered_map<string, string> extra_headers;
	optional_ptr<HTTPLogger> logger;

public:
	void Initialize(DatabaseInstance &db);
	void Initialize(ClientContext &context);

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

enum class RequestType : uint8_t { GET_REQUEST, PUT_REQUEST, HEAD_REQUEST, DELETE_REQUEST, POST_REQUEST };

struct HTTPHeaders {
	using header_map_t = case_insensitive_map_t<string>;

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
	header_map_t::mapped_type &operator[](const header_map_t::key_type &key) {
		return headers[key];
	}

private:
	header_map_t headers;
};

struct HTTPResponse {
	explicit HTTPResponse(HTTPStatusCode code);

	HTTPStatusCode status;
	string url;
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
	const string &GetError() const;

	bool ShouldRetry() const;
};

struct BaseRequest {
	BaseRequest(RequestType type, const string &url, const HTTPHeaders &headers, HTTPParams &params);
	BaseRequest(RequestType type, const string &endpoint_p, const string &path_p, const HTTPHeaders &headers,
	            HTTPParams &params)
	    : type(type), url(path), path(path_p), proto_host_port(endpoint_p), headers(headers), params(params) {
	}

	RequestType type;
	const string &url;
	string path;
	string proto_host_port;
	const HTTPHeaders &headers;
	HTTPParams &params;
	//! Whether or not to return failed requests (instead of throwing)
	bool try_request = false;

	template <class TARGET>
	TARGET &Cast() {
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct GetRequestInfo : public BaseRequest {
	GetRequestInfo(const string &url, const HTTPHeaders &headers, HTTPParams &params,
	               std::function<bool(const HTTPResponse &response)> response_handler_p,
	               std::function<bool(const_data_ptr_t data, idx_t data_length)> content_handler_p)
	    : BaseRequest(RequestType::GET_REQUEST, url, headers, params), content_handler(std::move(content_handler_p)),
	      response_handler(std::move(response_handler_p)) {
	}
	GetRequestInfo(const string &endpoint, const string &path, const HTTPHeaders &headers, HTTPParams &params,
	               std::function<bool(const HTTPResponse &response)> response_handler_p,
	               std::function<bool(const_data_ptr_t data, idx_t data_length)> content_handler_p)
	    : BaseRequest(RequestType::GET_REQUEST, endpoint, path, headers, params),
	      content_handler(std::move(content_handler_p)), response_handler(std::move(response_handler_p)) {
	}

	std::function<bool(const_data_ptr_t data, idx_t data_length)> content_handler;
	std::function<bool(const HTTPResponse &response)> response_handler;
};

struct PutRequestInfo : public BaseRequest {
	PutRequestInfo(const string &path, const HTTPHeaders &headers, HTTPParams &params, const_data_ptr_t buffer_in,
	               idx_t buffer_in_len, const string &content_type)
	    : BaseRequest(RequestType::PUT_REQUEST, path, headers, params), buffer_in(buffer_in),
	      buffer_in_len(buffer_in_len), content_type(content_type) {
	}

	const_data_ptr_t buffer_in;
	idx_t buffer_in_len;
	const string &content_type;
};

struct HeadRequestInfo : public BaseRequest {
	HeadRequestInfo(const string &path, const HTTPHeaders &headers, HTTPParams &params)
	    : BaseRequest(RequestType::HEAD_REQUEST, path, headers, params) {
	}
};

struct DeleteRequestInfo : public BaseRequest {
	DeleteRequestInfo(const string &path, const HTTPHeaders &headers, HTTPParams &params)
	    : BaseRequest(RequestType::DELETE_REQUEST, path, headers, params) {
	}
};

struct PostRequestInfo : public BaseRequest {
	PostRequestInfo(const string &path, const HTTPHeaders &headers, HTTPParams &params, const_data_ptr_t buffer_in,
	                idx_t buffer_in_len)
	    : BaseRequest(RequestType::POST_REQUEST, path, headers, params), buffer_in(buffer_in),
	      buffer_in_len(buffer_in_len) {
	}

	const_data_ptr_t buffer_in;
	idx_t buffer_in_len;
	string buffer_out;
};

class HTTPClient {
public:
	virtual ~HTTPClient() = default;

	virtual unique_ptr<HTTPResponse> Get(GetRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Put(PutRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Post(PostRequestInfo &info) = 0;

	unique_ptr<HTTPResponse> Request(BaseRequest &request);
};

class HTTPUtil {
public:
	virtual ~HTTPUtil() = default;

public:
	static HTTPUtil &Get(DatabaseInstance &db);

	virtual unique_ptr<HTTPClient> InitializeClient(HTTPParams &http_params, const string &proto_host_port);

	unique_ptr<HTTPResponse> Request(BaseRequest &request);
	unique_ptr<HTTPResponse> Request(BaseRequest &request, unique_ptr<HTTPClient> &client);

	virtual unique_ptr<HTTPResponse> SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &client);

	static void ParseHTTPProxyHost(string &proxy_value, string &hostname_out, idx_t &port_out, idx_t default_port = 80);
	static void DecomposeURL(const string &url, string &path_out, string &proto_host_port_out);
	static HTTPStatusCode ToStatusCode(int32_t status_code);

public:
	static duckdb::unique_ptr<HTTPResponse>
	RunRequestWithRetry(const std::function<unique_ptr<HTTPResponse>(void)> &on_request, const BaseRequest &request,
	                    const std::function<void(void)> &retry_cb = {});
};
} // namespace duckdb
