//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

constexpr uint64_t VERSION_NUMBER = 1;
constexpr uint64_t HEADER_SIZE = 4096;

struct MainHeader {
	uint64_t version_number;
	uint64_t flags[4];
};

struct DatabaseHeader {
	uint64_t iteration;
	int64_t meta_block;
	int64_t free_list;
};

}
