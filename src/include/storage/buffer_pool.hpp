//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/buffer_pool.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include "storage/page.hpp"

namespace duckdb {

class BufferPool {
	Page *CreatePage();
	void LoadPage(int64_t page_identifier);

	std::mutex loading_lock;
	std::unordered_map<int64_t, Page *> loading_pages;
	PageQueue cooling_queue;
};

} // namespace duckdb
