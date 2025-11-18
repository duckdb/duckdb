#include "duckdb/common/http_util.hpp"

#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_file_opener.hpp"

#ifndef DISABLE_DUCKDB_REMOTE_INSTALL
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
#include "httplib.hpp"
#endif
#endif
#ifndef DUCKDB_NO_THREADS
#include <chrono>
#include <thread>
#endif

namespace duckdb {

HTTPParams::~HTTPParams() {
}

HTTPHeaders::HTTPHeaders(DatabaseInstance &db) {
	headers.insert({"User-Agent", StringUtil::Format("%s %s", db.config.UserAgent(), DuckDB::SourceID())});
}

void HTTPHeaders::Insert(string key, string value) {
	headers.insert(make_pair(std::move(key), std::move(value)));
}

bool HTTPHeaders::HasHeader(const string &key) const {
	return headers.find(key) != headers.end();
}

string HTTPHeaders::GetHeaderValue(const string &key) const {
	auto entry = headers.find(key);
	if (entry == headers.end()) {
		throw InternalException("Header value not found");
	}
	return entry->second;
}

unique_ptr<HTTPResponse> TransformResponse(duckdb_httplib::Result &res) {
	auto status_code = HTTPUtil::ToStatusCode(res ? res->status : 0);
	auto result = make_uniq<HTTPResponse>(status_code);
	if (res.error() == duckdb_httplib::Error::Success) {
		auto &response = res.value();
		result->body = response.body;
		result->reason = response.reason;
		for (auto &entry : response.headers) {
			result->headers.Insert(entry.first, entry.second);
		}
	} else {
		result->request_error = to_string(res.error());
	}
	return result;
}

HTTPResponse::HTTPResponse(HTTPStatusCode code) : status(code) {
}

bool HTTPResponse::HasHeader(const string &key) const {
	return headers.HasHeader(key);
}

string HTTPResponse::GetHeaderValue(const string &key) const {
	return headers.GetHeaderValue(key);
}

bool HTTPResponse::Success() const {
	return success;
}

bool HTTPResponse::HasRequestError() const {
	return !request_error.empty();
}

const string &HTTPResponse::GetRequestError() const {
	return request_error;
}

const string &HTTPResponse::GetError() const {
	return request_error.empty() ? reason : request_error;
}

HTTPUtil &HTTPUtil::Get(DatabaseInstance &db) {
	return *db.config.http_util;
}

string HTTPUtil::GetName() const {
	return "Built-In";
}

bool HTTPResponse::ShouldRetry() const {
	if (HasRequestError()) {
		// always retry on request errors
		return true;
	}
	switch (status) {
	case HTTPStatusCode::RequestTimeout_408:
	case HTTPStatusCode::ImATeapot_418:
	case HTTPStatusCode::TooManyRequests_429:
	case HTTPStatusCode::InternalServerError_500:
	case HTTPStatusCode::ServiceUnavailable_503:
	case HTTPStatusCode::GatewayTimeout_504:
		return true;
	default:
		return false;
	}
}

unique_ptr<HTTPResponse> HTTPUtil::Request(BaseRequest &request) {
	unique_ptr<HTTPClient> client;
	return SendRequest(request, client);
}

unique_ptr<HTTPResponse> HTTPUtil::Request(BaseRequest &request, unique_ptr<HTTPClient> &client) {
	return SendRequest(request, client);
}

BaseRequest::BaseRequest(RequestType type, const string &url, const HTTPHeaders &headers, HTTPParams &params)
    : type(type), url(url), headers(headers), params(params) {
	HTTPUtil::DecomposeURL(url, path, proto_host_port);
}

class HTTPLibClient : public HTTPClient {
public:
	HTTPLibClient(HTTPParams &http_params, const string &proto_host_port) {
		client = make_uniq<duckdb_httplib::Client>(proto_host_port);
		Initialize(http_params);
	}
	void Initialize(HTTPParams &http_params) override {
		auto sec = static_cast<time_t>(http_params.timeout);
		auto usec = static_cast<time_t>(http_params.timeout_usec);
		client->set_follow_location(http_params.follow_location);
		client->set_keep_alive(http_params.keep_alive);
		client->set_write_timeout(sec, usec);
		client->set_read_timeout(sec, usec);
		client->set_connection_timeout(sec, usec);
		client->set_decompress(false);

		if (!http_params.http_proxy.empty()) {
			client->set_proxy(http_params.http_proxy, static_cast<int>(http_params.http_proxy_port));

			if (!http_params.http_proxy_username.empty()) {
				client->set_proxy_basic_auth(http_params.http_proxy_username, http_params.http_proxy_password);
			}
		}
	}
	unique_ptr<HTTPResponse> Get(GetRequestInfo &info) override {
		auto headers = TransformHeaders(info.headers, info.params);
		if (!info.response_handler && !info.content_handler) {
			return TransformResult(client->Get(info.path, headers));
		} else {
			return TransformResult(client->Get(
			    info.path, headers,
			    [&](const duckdb_httplib::Response &response) {
				    auto http_response = TransformResponse(response);
				    return info.response_handler(*http_response);
			    },
			    [&](const char *data, size_t data_length) {
				    return info.content_handler(const_data_ptr_cast(data), data_length);
			    }));
		}
	}
	unique_ptr<HTTPResponse> Put(PutRequestInfo &info) override {
		throw NotImplementedException("PUT request not implemented");
	}

	unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) override {
		throw NotImplementedException("HEAD request not implemented");
	}

	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &info) override {
		throw NotImplementedException("DELETE request not implemented");
	}

	unique_ptr<HTTPResponse> Post(PostRequestInfo &info) override {
		throw NotImplementedException("POST request not implemented");
	}

	unique_ptr<duckdb_httplib::Client> client;

private:
	duckdb_httplib::Headers TransformHeaders(const HTTPHeaders &header_map, const HTTPParams &params) {
		duckdb_httplib::Headers headers;
		for (auto &entry : header_map) {
			headers.insert(entry);
		}
		for (auto &entry : params.extra_headers) {
			headers.insert(entry);
		}
		return headers;
	}

	unique_ptr<HTTPResponse> TransformResponse(const duckdb_httplib::Response &response) {
		auto status_code = HTTPUtil::ToStatusCode(response.status);
		auto result = make_uniq<HTTPResponse>(status_code);
		result->body = response.body;
		result->reason = response.reason;
		for (auto &entry : response.headers) {
			result->headers.Insert(entry.first, entry.second);
		}
		return result;
	}

	unique_ptr<HTTPResponse> TransformResult(const duckdb_httplib::Result &res) {
		if (res.error() == duckdb_httplib::Error::Success) {
			auto &response = res.value();
			return TransformResponse(response);
		} else {
			auto result = make_uniq<HTTPResponse>(HTTPStatusCode::INVALID);
			result->request_error = to_string(res.error());
			return result;
		}
	}
};

unique_ptr<HTTPClient> HTTPUtil::InitializeClient(HTTPParams &http_params, const string &proto_host_port) {
	return make_uniq<HTTPLibClient>(http_params, proto_host_port);
}

unique_ptr<HTTPResponse> HTTPUtil::SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &client) {
	if (!client) {
		client = InitializeClient(request.params, request.proto_host_port);
	}

	std::function<unique_ptr<HTTPResponse>(void)> on_request([&]() {
		unique_ptr<HTTPResponse> response;

		// When logging is enabled, we collect request timings
		if (request.params.logger) {
			request.have_request_timing = request.params.logger->ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL);
		}

		try {
			if (request.have_request_timing) {
				request.request_start = Timestamp::GetCurrentTimestamp();
			}
			response = client->Request(request);
		} catch (...) {
			if (request.have_request_timing) {
				request.request_end = Timestamp::GetCurrentTimestamp();
			}
			LogRequest(request, nullptr);
			throw;
		}
		if (request.have_request_timing) {
			request.request_end = Timestamp::GetCurrentTimestamp();
		}
		LogRequest(request, response ? response.get() : nullptr);
		return response;
	});

	// Refresh the client on retries
	std::function<void(void)> on_retry([&]() { client = InitializeClient(request.params, request.proto_host_port); });

	return RunRequestWithRetry(on_request, request, on_retry);
}

void HTTPUtil::LogRequest(BaseRequest &request, optional_ptr<HTTPResponse> response) {
	if (!request.params.logger || !request.params.logger->ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL)) {
		return;
	}
	auto log_string = HTTPLogType::ConstructLogMessage(request, response);
	request.params.logger->WriteLog(HTTPLogType::NAME, HTTPLogType::LEVEL, log_string);
}

void HTTPUtil::ParseHTTPProxyHost(string &proxy_value, string &hostname_out, idx_t &port_out, idx_t default_port) {
	auto sanitized_proxy_value = proxy_value;
	if (StringUtil::StartsWith(proxy_value, "http://")) {
		sanitized_proxy_value = proxy_value.substr(7);
	}
	auto proxy_split = StringUtil::Split(sanitized_proxy_value, ":");
	if (proxy_split.size() == 1) {
		hostname_out = proxy_split[0];
		port_out = default_port;
	} else if (proxy_split.size() == 2) {
		idx_t port;
		if (!TryCast::Operation<string_t, idx_t>(proxy_split[1], port, false)) {
			throw InvalidInputException("Failed to parse port from http_proxy '%s'", proxy_value);
		}
		hostname_out = proxy_split[0];
		port_out = port;
	} else {
		throw InvalidInputException("Failed to parse http_proxy '%s' into a host and port", proxy_value);
	}
}

namespace {

enum class URISchemeType { HTTP, HTTPS, NONE, OTHER };

struct URISchemeDetectionResult {
	string lower_scheme;
	URISchemeType scheme_type = URISchemeType::NONE;
};

bool IsValidSchemeChar(char c) {
	return std::isalnum(c) || c == '+' || c == '.' || c == '-';
}

//! See https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
URISchemeDetectionResult DetectURIScheme(const string &uri) {
	URISchemeDetectionResult result;
	auto colon_pos = uri.find(':');

	// No colon or it's before any non-scheme content
	if (colon_pos == string::npos || colon_pos == 0) {
		result.lower_scheme = "";
		result.scheme_type = URISchemeType::NONE;
		return result;
	}

	if (!std::isalpha(uri[0])) {
		//! Scheme names consist of a sequence of characters beginning with a letter
		result.lower_scheme = "";
		result.scheme_type = URISchemeType::NONE;
		return result;
	}

	// Validate scheme characters
	for (size_t i = 1; i < colon_pos; ++i) {
		if (!IsValidSchemeChar(uri[i])) {
			//! Scheme can't contain this character, assume the URI has no scheme
			result.lower_scheme = "";
			result.scheme_type = URISchemeType::NONE;
			return result;
		}
	}

	string scheme = uri.substr(0, colon_pos);
	result.lower_scheme = StringUtil::Lower(scheme);

	if (result.lower_scheme == "http") {
		result.scheme_type = URISchemeType::HTTP;
		return result;
	}
	if (result.lower_scheme == "https") {
		result.scheme_type = URISchemeType::HTTPS;
		return result;
	}
	result.scheme_type = URISchemeType::OTHER;
	return result;
}

} // namespace

void HTTPUtil::DecomposeURL(const string &input, string &path_out, string &proto_host_port_out) {
	auto detection_result = DetectURIScheme(input);
	auto url = input;
	if (detection_result.scheme_type == URISchemeType::NONE) {
		//! Assume it's HTTP
		url = "http://" + url;
	}

	auto slash_pos = url.find('/', 8);
	if (slash_pos == string::npos) {
		throw IOException("URL needs to contain a '/' after the host");
	}
	proto_host_port_out = url.substr(0, slash_pos);

	path_out = url.substr(slash_pos);

	if (path_out.empty()) {
		throw IOException("URL needs to contain a path");
	}
}

// Retry the request performed by fun using the exponential backoff strategy defined in params. Before retry, the
// retry callback is called
duckdb::unique_ptr<HTTPResponse>
HTTPUtil::RunRequestWithRetry(const std::function<unique_ptr<HTTPResponse>(void)> &on_request,
                              const BaseRequest &request, const std::function<void(void)> &retry_cb) {
	auto &params = request.params;
	idx_t tries = 0;
	while (true) {
		std::exception_ptr caught_e = nullptr;
		unique_ptr<HTTPResponse> response;
		string exception_error;

		try {
			response = on_request();
			if (response) {
				response->url = request.url;
			}
		} catch (IOException &e) {
			exception_error = e.what();
			caught_e = std::current_exception();
		} catch (HTTPException &e) {
			exception_error = e.what();
			caught_e = std::current_exception();
		}

		// Note: request errors will always be retried
		bool should_retry = !response || response->ShouldRetry();
		if (!should_retry) {
			auto response_code = static_cast<uint16_t>(response->status);
			if (response_code >= 200 && response_code < 300) {
				response->success = true;
				return response;
			}
			switch (response->status) {
			case HTTPStatusCode::NotModified_304:
				response->success = true;
				break;
			default:
				response->success = false;
				break;
			}
			return response;
		}

		tries += 1;
		if (tries <= params.retries) {
			if (tries > 1) {
#ifndef DUCKDB_NO_THREADS
				uint64_t sleep_amount = (uint64_t)((double)params.retry_wait_ms *
				                                   pow(params.retry_backoff, static_cast<double>(tries - 2)));
				std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
#endif
			}
			if (retry_cb) {
				retry_cb();
			}
		} else {
			// failed and we cannot retry
			if (request.try_request) {
				// try request - return the failure
				if (!response) {
					response = make_uniq<HTTPResponse>(HTTPStatusCode::INVALID);
					string error = "Unknown error";
					if (!exception_error.empty()) {
						error = std::move(exception_error);
					}
					response->request_error = std::move(error);
				}
				response->success = false;
				return response;
			}
			auto method = EnumUtil::ToString(request.type);
			if (caught_e) {
				std::rethrow_exception(caught_e);
			} else if (response && !response->HasRequestError()) {
				throw HTTPException(*response, "Request returned HTTP %d for HTTP %s to '%s'",
				                    static_cast<int>(response->status), method, request.url);
			} else {
				string error = response ? response->GetError() : "Unknown error";
				throw IOException("%s error for HTTP %s to '%s'", error, method, request.url);
			}
		}
	}
}

void HTTPParams::Initialize(optional_ptr<FileOpener> opener) {
	auto db = FileOpener::TryGetDatabase(opener);
	if (db) {
		auto &config = db->config;
		if (!config.options.http_proxy.empty()) {
			idx_t port;
			string host;
			HTTPUtil::ParseHTTPProxyHost(config.options.http_proxy, host, port);
			http_proxy = host;
			http_proxy_port = port;
		}
		http_proxy_username = config.options.http_proxy_username;
		http_proxy_password = config.options.http_proxy_password;
	}

	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context) {
		auto &client_config = ClientConfig::GetConfig(*client_context);
		if (client_config.enable_http_logging) {
			logger = client_context->logger;
		}
	}
}

unique_ptr<HTTPParams> HTTPUtil::InitializeParameters(DatabaseInstance &db, const string &url) {
	DatabaseFileOpener opener(db);
	FileOpenerInfo info;
	info.file_path = url;
	return InitializeParameters(&opener, &info);
}

unique_ptr<HTTPParams> HTTPUtil::InitializeParameters(ClientContext &context, const string &url) {
	ClientContextFileOpener opener(context);
	FileOpenerInfo info;
	info.file_path = url;
	return InitializeParameters(&opener, &info);
}

unique_ptr<HTTPParams> HTTPUtil::InitializeParameters(optional_ptr<FileOpener> opener,
                                                      optional_ptr<FileOpenerInfo> info) {
	auto result = make_uniq<HTTPParams>(*this);
	result->Initialize(opener);
	return result;
}

unique_ptr<HTTPResponse> HTTPClient::Request(BaseRequest &request) {
	switch (request.type) {
	case RequestType::GET_REQUEST:
		return Get(request.Cast<GetRequestInfo>());
	case RequestType::PUT_REQUEST:
		return Put(request.Cast<PutRequestInfo>());
	case RequestType::HEAD_REQUEST:
		return Head(request.Cast<HeadRequestInfo>());
	case RequestType::DELETE_REQUEST:
		return Delete(request.Cast<DeleteRequestInfo>());
	case RequestType::POST_REQUEST:
		return Post(request.Cast<PostRequestInfo>());
	default:
		throw InternalException("Unsupported request type");
	}
}

} // namespace duckdb
