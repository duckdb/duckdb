//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/storage_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "constants.hpp"
#include <cassert>

namespace duckdb {
constexpr const size_t FULL_MASK = 0xFF;
class StorageUtil {

	//! Checksum using longitudinal redundancy check algorithm adepted from
	//! https://en.wikipedia.org/wiki/Longitudinal_redundancy_check
	static uint8_t CalculateCheckSum(duckdb::data_ptr buffer, size_t buffer_size) {
		assert(buffer != nullptr && buffer_size > 0);
		uint8_t lrc = 0;
		for (size_t i = 0; i != buffer_size; ++i) {
			lrc = ((lrc + buffer[i]) & FULL_MASK);
		}
		lrc = (((lrc ^ FULL_MASK) + 1) & FULL_MASK);
		return lrc;
	}

	static bool IsValidCheckSum(duckdb::data_ptr buffer, size_t buffer_size, uint8_t input_checksum) {
		auto obtained_checksum = CalculateCheckSum(move(buffer), buffer_size);
		return obtained_checksum == input_checksum;
	}
};
} // namespace duckdb
