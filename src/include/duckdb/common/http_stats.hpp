//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/http_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

class HTTPStats {
public:
	atomic<idx_t> head_count {0};
	atomic<idx_t> get_count {0};
	atomic<idx_t> put_count {0};
	atomic<idx_t> post_count {0};
	atomic<idx_t> total_bytes_received {0};
	atomic<idx_t> total_bytes_sent {0};

	void Reset() {
		head_count = 0;
		get_count = 0;
		put_count = 0;
		post_count = 0;
		total_bytes_received = 0;
	}

	//! helper function to get the HTTP
	static HTTPStats* TryGetStats(FileOpener * opener) {
		auto client_context = opener->TryGetClientContext();
		if (client_context) {
			return client_context->client_data->http_stats.get();
		}
		return nullptr;
	}
};

} // namespace duckdb
