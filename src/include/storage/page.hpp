//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/page.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>
#include <vector>

namespace duckdb {

// Size of pages, 16K
#define PAGE_SIZE 16384

typedef union {
	struct {
		int64_t is_swizzled : 1;
		int64_t page_identifier : 63;
	} swizzled_pointer;
	void *unswizzled_pointer;
} swizzled_pointer_t;

class Page {
	//! The swizzled pointer to the next page
	swizzled_pointer_t next;
	//! Whether or not the page is dirty
	bool dirty;
	//! The actual data of the page
	char data[PAGE_SIZE];
};

} // namespace duckdb
