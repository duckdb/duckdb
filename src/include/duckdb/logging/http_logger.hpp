//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/http_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/fstream.hpp"
#include "duckdb/main/client_context.hpp"

#include <functional>

namespace duckdb {

//! This has to be templated because we have two namespaces:
//! 1. duckdb_httplib
//! 2. duckdb_httplib_openssl
//! These have essentially the same code, but we cannot convert between them
//! We get around that by templating everything, which requires implementing everything in the header
class HTTPLogger {
public:
	static constexpr const char *HTTP_LOGGER_LOG_TYPE = "duckdb.Httplib";
	static constexpr LogLevel HTTP_LOGGER_LOG_LEVEL = LogLevel::LOG_DEBUG;

	explicit HTTPLogger(ClientContext &context_p) : context(context_p.shared_from_this()) {
	}

	static bool ShouldLog(ClientContext &context) {
		// note: legacy option that should probably just always be enabled now that we have a proper logger:
		//       ShouldLog will determine correctly what to do here
		if (!context.config.enable_http_logging) {
			return false;
		}
		return Logger::Get(context).ShouldLog(HTTP_LOGGER_LOG_TYPE, HTTP_LOGGER_LOG_LEVEL);
	}

public:
	template <class REQUEST, class RESPONSE>
	std::function<void(const REQUEST &, const RESPONSE &)> GetLogger() {
		return [&](const REQUEST &req, const RESPONSE &res) {
			Log(req, res);
		};
	}

	// to be used to determine whether the httplib logger should be injected in the httplib clients
	bool ShouldLog() {
		auto context_ptr = context.lock();
		if (!context_ptr) {
			return false;
		}
		return ShouldLog(*context_ptr);
	}

private:
	template <class STREAM, class REQUEST, class RESPONSE>
	static inline void TemplatedWriteRequests(STREAM &out, const REQUEST &req, const RESPONSE &res) {
		out << "HTTP Request:\n";
		out << "\t" << req.method << " " << req.path << "\n";
		for (auto &entry : req.headers) {
			out << "\t" << entry.first << ": " << entry.second << "\n";
		}
		out << "\nHTTP Response:\n";
		out << "\t" << res.status << " " << res.reason << " " << req.version << "\n";
		for (auto &entry : res.headers) {
			out << "\t" << entry.first << ": " << entry.second << "\n";
		}
		out << "\n";
	}

	template <class REQUEST, class RESPONSE>
	void Log(const REQUEST &req, const RESPONSE &res) {
		auto context_ptr = context.lock();
		if (!context_ptr) {
			return;
		}
		// This is a deprecated path, but we might as well support it
		if (!context_ptr->config.http_logging_output.empty()) {
			ofstream out(context_ptr->config.http_logging_output, ios::app);
			TemplatedWriteRequests(out, req, res);
			out.close();
			// Throw an IO exception if it fails to write to the file
			if (out.fail()) {
				throw IOException("Failed to write HTTP log to file \"%s\": %s",
				                  context_ptr->config.http_logging_output, strerror(errno));
			}
		}

		auto &logger = Logger::Get(*context_ptr);
		if (logger.ShouldLog(HTTP_LOGGER_LOG_TYPE, HTTP_LOGGER_LOG_LEVEL)) {
			stringstream out;
			TemplatedWriteRequests(out, req, res);
			logger.WriteLog(HTTP_LOGGER_LOG_TYPE, HTTP_LOGGER_LOG_LEVEL, out.str());
		}
	}

private:
	weak_ptr<ClientContext> context;
};

} // namespace duckdb
