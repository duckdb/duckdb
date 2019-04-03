//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/buffer_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "storage/page.hpp"

#include <atomic>
#include <mutex>
#include <vector>

namespace duckdb {

class BufferPool {
	Page *CreatePage();
	void LoadPage(int64_t page_identifier);

	std::mutex loading_lock;
	unordered_map<int64_t, Page *> loading_pages;
	PageQueue cooling_queue;
};

} // namespace duckdb
