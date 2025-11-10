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
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include <functional>

namespace duckdb {
class DatabaseInstance;
class Logger;
class HTTPUtil;
class FileOpener;
struct FileOpenerInfo;

struct HTTPLogWriter {};

struct HTTPParams {
	explicit HTTPParams(HTTPUtil &http_util) : http_util(http_util) {
	}
	virtual ~HTTPParams();

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
	bool follow_location = true;

	string http_proxy;
	idx_t http_proxy_port;
	string http_proxy_username;
	string http_proxy_password;
	unordered_map<string, string> extra_headers;
	HTTPUtil &http_util;
	shared_ptr<Logger> logger;

public:
	void Initialize(optional_ptr<FileOpener> opener);

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

	// Requests will optionally contain their timings
	bool have_request_timing = false;
	timestamp_t request_start;
	timestamp_t request_end;

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
	virtual void Initialize(HTTPParams &http_params) = 0;

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

	virtual string GetName() const;

	virtual unique_ptr<HTTPParams> InitializeParameters(DatabaseInstance &db, const string &path);
	virtual unique_ptr<HTTPParams> InitializeParameters(ClientContext &context, const string &path);
	virtual unique_ptr<HTTPParams> InitializeParameters(optional_ptr<FileOpener> opener,
	                                                    optional_ptr<FileOpenerInfo> info);

	virtual unique_ptr<HTTPClient> InitializeClient(HTTPParams &http_params, const string &proto_host_port);

	unique_ptr<HTTPResponse> Request(BaseRequest &request);
	unique_ptr<HTTPResponse> Request(BaseRequest &request, unique_ptr<HTTPClient> &client);

	virtual unique_ptr<HTTPResponse> SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &client);
	virtual void LogRequest(BaseRequest &request, optional_ptr<HTTPResponse> response);

	static void ParseHTTPProxyHost(string &proxy_value, string &hostname_out, idx_t &port_out, idx_t default_port = 80);
	static void DecomposeURL(const string &url, string &path_out, string &proto_host_port_out);
	static HTTPStatusCode ToStatusCode(int32_t status_code);
	static string GetStatusMessage(HTTPStatusCode status);

public:
	static duckdb::unique_ptr<HTTPResponse>
	RunRequestWithRetry(const std::function<unique_ptr<HTTPResponse>(void)> &on_request, const BaseRequest &request,
	                    const std::function<void(void)> &retry_cb);
};
} // namespace duckdb
