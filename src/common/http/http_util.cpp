#include "duckdb/common/http_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/http_logger.hpp"
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

unique_ptr<HTTPResponse> TransformResponse(duckdb_httplib::Result &res) {
	auto result = make_uniq<HTTPResponse>(static_cast<HTTPStatusCode>(res->status));
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

HTTPUtil &HTTPUtil::Get(DatabaseInstance &db) {
	return *db.config.http_util;
}

unique_ptr<HTTPResponse> HTTPUtil::Request(DatabaseInstance &db, const string &url, const HTTPHeaders &headers,
                                           optional_ptr<HTTPLogger> http_logger) {
	string no_http = StringUtil::Replace(url, "http://", "");

	idx_t next = no_http.find('/', 0);
	if (next == string::npos) {
		throw IOException("No slash in URL template");
	}

	// Push the substring [last, next) on to splits
	auto hostname_without_http = no_http.substr(0, next);
	auto url_local_part = no_http.substr(next);

	auto url_base = "http://" + hostname_without_http;

	duckdb_httplib::Headers httplib_headers;
	for (auto &header : headers) {
		httplib_headers.insert({header.first, header.second});
	}

	// FIXME: the retry logic should be unified with the retry logic in the httpfs client
	static constexpr idx_t MAX_RETRY_COUNT = 3;
	static constexpr uint64_t RETRY_WAIT_MS = 100;
	static constexpr double RETRY_BACKOFF = 4;
	idx_t retry_count = 0;
	duckdb_httplib::Result res;
	while (true) {
		duckdb_httplib::Client cli(url_base.c_str());
		if (!db.config.options.http_proxy.empty()) {
			idx_t port;
			string host;
			HTTPUtil::ParseHTTPProxyHost(db.config.options.http_proxy, host, port);
			cli.set_proxy(host, NumericCast<int>(port));
		}

		if (!db.config.options.http_proxy_username.empty() || !db.config.options.http_proxy_password.empty()) {
			cli.set_proxy_basic_auth(db.config.options.http_proxy_username, db.config.options.http_proxy_password);
		}

		if (http_logger) {
			cli.set_logger(http_logger->GetLogger<duckdb_httplib::Request, duckdb_httplib::Response>());
		}

		res = cli.Get(url_local_part.c_str(), httplib_headers);
		if (res->status == 304) {
			return make_uniq<HTTPResponse>(HTTPStatusCode::NotModified_304);
		}
		if (res && res->status == 200) {
			// success!
			return TransformResponse(res);
		}
		// failure - check if we should retry
		bool should_retry = false;
		if (res.error() == duckdb_httplib::Error::Success) {
			switch (res->status) {
			case 408: // Request Timeout
			case 418: // Server is pretending to be a teapot
			case 429: // Rate limiter hit
			case 500: // Server has error
			case 503: // Server has error
			case 504: // Server has error
				should_retry = true;
				break;
			default:
				break;
			}
		} else {
			// always retry on duckdb_httplib::Error::Error
			should_retry = true;
		}
		retry_count++;
		if (!should_retry || retry_count >= MAX_RETRY_COUNT) {
			// if we should not retry or exceeded the number of retries - bubble up the error
			auto result = TransformResponse(res);
			result->success = false;
			return result;
		}
#ifndef DUCKDB_NO_THREADS
		// retry
		// sleep first
		uint64_t sleep_amount = static_cast<uint64_t>(static_cast<double>(RETRY_WAIT_MS) *
		                                              pow(RETRY_BACKOFF, static_cast<double>(retry_count) - 1));
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
#endif
	}
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

} // namespace duckdb
