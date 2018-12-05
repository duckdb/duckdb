//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/fstream_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "common/exception.hpp"

#include <fstream>
#include <iostream>

namespace duckdb {
/**
 * Fstream Utility Functions
 */
class FstreamUtil {
public:
	/**
	 * Returns true if the needle string exists in the haystack
	 */
	std::fstream OpenFile(const string &file_path);
	size_t GetFileSize(std::fstream &file);
	unique_ptr<char[]> ReadBinary(std::fstream &file);
};
} // namespace duckdb
