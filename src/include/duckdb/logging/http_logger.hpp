//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/http_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"

#include <functional>

namespace duckdb_httplib {
struct Request;
struct Response;
} // namespace duckdb_httplib

namespace duckdb {

class ClientContext;

class HTTPLogger {
public:
	explicit HTTPLogger(ClientContext &context);

public:
	std::function<void(const duckdb_httplib::Request &, const duckdb_httplib::Response &)> GetLogger();

private:
	void Log(const duckdb_httplib::Request &req, const duckdb_httplib::Response &res);

private:
	ClientContext &context;
	mutex lock;
};

} // namespace duckdb
