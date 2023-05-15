//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/http_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct CachedFile {
	//! Cached Data
	shared_ptr<char> data;
	//! Data capacity
	uint64_t capacity = 0;
	//! If we finished downloading the file
	bool finished = false;
};

class HTTPState {
public:
	atomic<idx_t> head_count {0};
	atomic<idx_t> get_count {0};
	atomic<idx_t> put_count {0};
	atomic<idx_t> post_count {0};
	atomic<idx_t> total_bytes_received {0};
	atomic<idx_t> total_bytes_sent {0};
	//! Mutex to lock when getting the cached file(Parallel Only)
	mutex cached_files_mutex;
	//! In case of fully downloading the file, the cached files of this query
	unordered_map<string, CachedFile> cached_files;

	void Reset() {
		head_count = 0;
		get_count = 0;
		put_count = 0;
		post_count = 0;
		total_bytes_received = 0;
		total_bytes_sent = 0;
		cached_files.clear();
	}

	//! helper function to get the HTTP
	static shared_ptr<HTTPState> TryGetState(FileOpener *opener) {
		auto client_context = FileOpener::TryGetClientContext(opener);
		if (client_context) {
			return client_context->client_data->http_state;
		}
		return nullptr;
	}

	bool IsEmpty() {
		return head_count == 0 && get_count == 0 && put_count == 0 && post_count == 0 && total_bytes_received == 0 &&
		       total_bytes_sent == 0;
	}
};

} // namespace duckdb
