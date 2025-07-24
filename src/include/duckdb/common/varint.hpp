//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/varint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/string.hpp"
#include <stdint.h>

#include "hugeint.hpp"
#include "types/varint.hpp"

namespace duckdb {

struct varint_t : public string_t { // NOLINT: use numeric casing
	ArenaAllocator *allocator;
	DUCKDB_API explicit varint_t(ArenaAllocator &allocator) : allocator(&allocator) {};
	DUCKDB_API varint_t(ArenaAllocator &allocator, uint32_t blob_size);
	DUCKDB_API varint_t(ArenaAllocator &allocator, const char *data, size_t len);
	varint_t() : allocator(nullptr) {
	}
	bool Initialized() const {
		return allocator;
	}
	// varint_t(ArenaAllocator &allocator, const char *data, uint32_t len): string_t(data, len) {};
	// DUCKDB_API varint_t(hugeint_t value);
	// DUCKDB_API varint_t(uhugeint_t value);
	// DUCKDB_API varint_t(string_t value);

	varint_t(const varint_t &rhs) = default;
	varint_t(varint_t &&other) = default;
	varint_t &operator=(const varint_t &rhs) = default;
	varint_t &operator=(varint_t &&rhs) = default;

	// DUCKDB_API string ToString() const;

	// // arithmetic operators
	DUCKDB_API varint_t operator*(const varint_t &rhs) const;

	// in-place operators
	DUCKDB_API varint_t &operator+=(const varint_t &rhs);

	DUCKDB_API varint_t &operator+=(const string_t &rhs);
	//! Reallocate the Varint 2x-ing its size
	void Reallocate();
	//! In case we have unnecessary extra 0's or 1's in our varint we trim them
	void Trim();

	template <class T>
	static varint_t FromInteger(ArenaAllocator &allocator, T int_value) {
		const bool is_negative = int_value < 0;
		// Determine the number of data bytes
		uint64_t abs_value;
		if (is_negative) {
			if (int_value == std::numeric_limits<T>::min()) {
				abs_value = static_cast<uint64_t>(std::numeric_limits<T>::max()) + 1;
			} else {
				abs_value = static_cast<uint64_t>(std::abs(static_cast<int64_t>(int_value)));
			}
		} else {
			abs_value = static_cast<uint64_t>(int_value);
		}
		uint32_t data_byte_size;
		if (abs_value != NumericLimits<uint64_t>::Maximum()) {
			data_byte_size = (abs_value == 0) ? 1 : static_cast<uint32_t>(std::ceil(std::log2(abs_value + 1) / 8.0));
		} else {
			data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value) / 8.0));
		}

		uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
		varint_t result(allocator, blob_size);
		// Don't we have to initialize the string_t here?
		auto writable_blob = result.GetDataWriteable();
		Varint::SetHeader(writable_blob, data_byte_size, is_negative);

		// Add data bytes to the blob, starting off after header bytes
		idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
		for (int i = static_cast<int>(data_byte_size) - 1; i >= 0; --i) {
			if (is_negative) {
				writable_blob[wb_idx++] = static_cast<char>(~(abs_value >> i * 8 & 0xFF));
			} else {
				writable_blob[wb_idx++] = static_cast<char>(abs_value >> i * 8 & 0xFF);
			}
		}
		result.Finalize();
		return result;
	}

	static varint_t FromBlob(ArenaAllocator &allocator, string_t value) {
		auto blob_size = static_cast<uint32_t>(value.GetSize());
		varint_t result(allocator, blob_size);
		// Don't we have to initialize the string_t here?
		auto writable_blob = result.GetDataWriteable();
		memcpy(writable_blob, value.GetData(), blob_size);
		result.Finalize();
		return result;
	}
};

} // namespace duckdb
