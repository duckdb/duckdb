//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/http_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

class HTTPUtil {
public:
	static void ParseHTTPProxyHost(string &proxy_value, string &hostname_out, idx_t &port_out, idx_t default_port = 80);
};
} // namespace duckdb
