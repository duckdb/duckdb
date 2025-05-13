#include "duckdb/common/http_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/http_logger.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/client_data.hpp"
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

HTTPStatusCode HTTPUtil::ToStatusCode(int32_t status_code) {
	switch (status_code) {
	case 100:
		return HTTPStatusCode::Continue_100;
	case 101:
		return HTTPStatusCode::SwitchingProtocol_101;
	case 102:
		return HTTPStatusCode::Processing_102;
	case 103:
		return HTTPStatusCode::EarlyHints_103;
	case 200:
		return HTTPStatusCode::OK_200;
	case 201:
		return HTTPStatusCode::Created_201;
	case 202:
		return HTTPStatusCode::Accepted_202;
	case 203:
		return HTTPStatusCode::NonAuthoritativeInformation_203;
	case 204:
		return HTTPStatusCode::NoContent_204;
	case 205:
		return HTTPStatusCode::ResetContent_205;
	case 206:
		return HTTPStatusCode::PartialContent_206;
	case 207:
		return HTTPStatusCode::MultiStatus_207;
	case 208:
		return HTTPStatusCode::AlreadyReported_208;
	case 226:
		return HTTPStatusCode::IMUsed_226;
	case 300:
		return HTTPStatusCode::MultipleChoices_300;
	case 301:
		return HTTPStatusCode::MovedPermanently_301;
	case 302:
		return HTTPStatusCode::Found_302;
	case 303:
		return HTTPStatusCode::SeeOther_303;
	case 304:
		return HTTPStatusCode::NotModified_304;
	case 305:
		return HTTPStatusCode::UseProxy_305;
	case 306:
		return HTTPStatusCode::unused_306;
	case 307:
		return HTTPStatusCode::TemporaryRedirect_307;
	case 308:
		return HTTPStatusCode::PermanentRedirect_308;
	case 400:
		return HTTPStatusCode::BadRequest_400;
	case 401:
		return HTTPStatusCode::Unauthorized_401;
	case 402:
		return HTTPStatusCode::PaymentRequired_402;
	case 403:
		return HTTPStatusCode::Forbidden_403;
	case 404:
		return HTTPStatusCode::NotFound_404;
	case 405:
		return HTTPStatusCode::MethodNotAllowed_405;
	case 406:
		return HTTPStatusCode::NotAcceptable_406;
	case 407:
		return HTTPStatusCode::ProxyAuthenticationRequired_407;
	case 408:
		return HTTPStatusCode::RequestTimeout_408;
	case 409:
		return HTTPStatusCode::Conflict_409;
	case 410:
		return HTTPStatusCode::Gone_410;
	case 411:
		return HTTPStatusCode::LengthRequired_411;
	case 412:
		return HTTPStatusCode::PreconditionFailed_412;
	case 413:
		return HTTPStatusCode::PayloadTooLarge_413;
	case 414:
		return HTTPStatusCode::UriTooLong_414;
	case 415:
		return HTTPStatusCode::UnsupportedMediaType_415;
	case 416:
		return HTTPStatusCode::RangeNotSatisfiable_416;
	case 417:
		return HTTPStatusCode::ExpectationFailed_417;
	case 418:
		return HTTPStatusCode::ImATeapot_418;
	case 421:
		return HTTPStatusCode::MisdirectedRequest_421;
	case 422:
		return HTTPStatusCode::UnprocessableContent_422;
	case 423:
		return HTTPStatusCode::Locked_423;
	case 424:
		return HTTPStatusCode::FailedDependency_424;
	case 425:
		return HTTPStatusCode::TooEarly_425;
	case 426:
		return HTTPStatusCode::UpgradeRequired_426;
	case 428:
		return HTTPStatusCode::PreconditionRequired_428;
	case 429:
		return HTTPStatusCode::TooManyRequests_429;
	case 431:
		return HTTPStatusCode::RequestHeaderFieldsTooLarge_431;
	case 451:
		return HTTPStatusCode::UnavailableForLegalReasons_451;
	case 500:
		return HTTPStatusCode::InternalServerError_500;
	case 501:
		return HTTPStatusCode::NotImplemented_501;
	case 502:
		return HTTPStatusCode::BadGateway_502;
	case 503:
		return HTTPStatusCode::ServiceUnavailable_503;
	case 504:
		return HTTPStatusCode::GatewayTimeout_504;
	case 505:
		return HTTPStatusCode::HttpVersionNotSupported_505;
	case 506:
		return HTTPStatusCode::VariantAlsoNegotiates_506;
	case 507:
		return HTTPStatusCode::InsufficientStorage_507;
	case 508:
		return HTTPStatusCode::LoopDetected_508;
	case 510:
		return HTTPStatusCode::NotExtended_510;
	case 511:
		return HTTPStatusCode::NetworkAuthenticationRequired_511;
	default:
		return HTTPStatusCode::INVALID;
	}
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
		auto sec = static_cast<time_t>(http_params.timeout);
		auto usec = static_cast<time_t>(http_params.timeout_usec);
		client = make_uniq<duckdb_httplib::Client>(proto_host_port);
		client->set_follow_location(true);
		client->set_keep_alive(http_params.keep_alive);
		client->set_write_timeout(sec, usec);
		client->set_read_timeout(sec, usec);
		client->set_connection_timeout(sec, usec);
		client->set_decompress(false);
		if (http_params.logger) {
			SetLogger(*http_params.logger);
		}

		if (!http_params.http_proxy.empty()) {
			client->set_proxy(http_params.http_proxy, static_cast<int>(http_params.http_proxy_port));

			if (!http_params.http_proxy_username.empty()) {
				client->set_proxy_basic_auth(http_params.http_proxy_username, http_params.http_proxy_password);
			}
		}
	}

	void SetLogger(HTTPLogger &logger) {
		client->set_logger(logger.GetLogger<duckdb_httplib::Request, duckdb_httplib::Response>());
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

	std::function<unique_ptr<HTTPResponse>(void)> on_request([&]() { return client->Request(request); });
	// Refresh the client on retries
	std::function<void(void)> on_retry([&]() { client = InitializeClient(request.params, request.proto_host_port); });

	return RunRequestWithRetry(on_request, request);
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

void HTTPUtil::DecomposeURL(const string &url, string &path_out, string &proto_host_port_out) {
	if (url.rfind("http://", 0) != 0 && url.rfind("https://", 0) != 0) {
		throw IOException("URL needs to start with http:// or https://");
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
			response->url = request.url;
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
			switch (response->status) {
			case HTTPStatusCode::OK_200:
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

void HTTPParams::Initialize(DatabaseInstance &db) {
	if (!db.config.options.http_proxy.empty()) {
		idx_t port;
		string host;
		HTTPUtil::ParseHTTPProxyHost(db.config.options.http_proxy, host, port);
		http_proxy = host;
		http_proxy_port = port;
	}
	http_proxy_username = db.config.options.http_proxy_username;
	http_proxy_password = db.config.options.http_proxy_password;
}

void HTTPParams::Initialize(ClientContext &context) {
	Initialize(*context.db);
	auto &client_config = ClientConfig::GetConfig(context);
	if (client_config.enable_http_logging) {
		logger = context.client_data->http_logger.get();
	}
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
