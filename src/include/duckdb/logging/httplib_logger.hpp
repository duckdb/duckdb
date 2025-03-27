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
#include "duckdb/logging/logger.hpp"

#include <functional>

namespace duckdb {

//! This class is used to hook up the httplib (./third_party/httplib) logger to the duckdb logger.
//! This has to be templated because we have two namespaces:
//! 1. duckdb_httplib
//! 2. duckdb_httplib_openssl
//! These have essentially the same code, but we cannot convert between them
//! We get around that by templating everything, which requires implementing everything in the header
class HTTPLibLogger {
public:
	explicit HTTPLibLogger(ClientContext &context)
	    : logger(context.logger), http_logging_output(context.config.http_logging_output) {
	}

	static bool ShouldLog(ClientContext &context_p) {
		// note: legacy option that should probably just always be enabled now that we have a proper logger:
		//       ShouldLog will determine correctly what to do here
		if (!context_p.config.enable_http_logging) {
			return false;
		}
		return Logger::Get(context_p).ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL);
	}

	// Warning: the callback is only valid as long as the HTTPLogger is alive
	template <class REQUEST, class RESPONSE>
	std::function<void(const REQUEST &, const RESPONSE &)> GetHTTPLibCallback() {
		return [&](const REQUEST &req, const RESPONSE &res) {
			Log(req, res);
		};
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
	static string TemplatedWriteRequestsToString(REQUEST req, RESPONSE res) {
		stringstream out;
		TemplatedWriteRequests(out, req, res);
		return out.str();
	}

	template <class REQUEST, class RESPONSE>
	void Log(const REQUEST &req, const RESPONSE &res) {
		// This is a deprecated path, but we might as well support it
		if (!http_logging_output.empty()) {
			ofstream out(http_logging_output, ios::app);
			TemplatedWriteRequests(out, req, res);
			out.close();
			// Throw an IO exception if it fails to write to the file
			if (out.fail()) {
				throw IOException("Failed to write HTTP log to file \"%s\": %s", http_logging_output, strerror(errno));
			}
		}

		DUCKDB_LOG(logger, HTTPLogType, TemplatedWriteRequestsToString(req, res));
	}

protected:
	shared_ptr<Logger> logger;
	const string http_logging_output;
};

} // namespace duckdb
