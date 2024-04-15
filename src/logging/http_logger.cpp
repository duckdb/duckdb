#include "duckdb/logging/http_logger.hpp"

#include "duckdb/common/fstream.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "httplib.hpp"

namespace duckdb {

HTTPLogger::HTTPLogger(ClientContext &context_p) : context(context_p) {
}

std::function<void(const duckdb_httplib::Request &, const duckdb_httplib::Response &)> HTTPLogger::GetLogger() {
	return [&](const duckdb_httplib::Request &req, const duckdb_httplib::Response &res) {
		Log(req, res);
	};
}

template <class STREAM>
static inline void TemplatedWriteRequests(STREAM &out, const duckdb_httplib::Request &req,
                                          const duckdb_httplib::Response &res) {
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

void HTTPLogger::Log(const duckdb_httplib::Request &req, const duckdb_httplib::Response &res) {
	const auto &config = ClientConfig::GetConfig(context);
	D_ASSERT(config.enable_http_logging);

	lock_guard<mutex> guard(lock);
	if (config.http_logging_output.empty()) {
		stringstream out;
		TemplatedWriteRequests(out, req, res);
		Printer::Print(out.str());
	} else {
		ofstream out(config.http_logging_output, ios::app);
		TemplatedWriteRequests(out, req, res);
		out.close();
		// Throw an IO exception if it fails to write to the file
		if (out.fail()) {
			throw IOException("Failed to write HTTP log to file \"%s\": %s", config.http_logging_output,
			                  strerror(errno));
		}
	}
}

}; // namespace duckdb
