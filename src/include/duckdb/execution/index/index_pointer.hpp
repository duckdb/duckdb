//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/index_pointer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class IndexPointer {
public:
	//! Bit-shifting
	static constexpr idx_t SHIFT_OFFSET = 32;
	static constexpr idx_t SHIFT_METADATA = 56;
	//! AND operations
	static constexpr idx_t AND_OFFSET = 0x0000000000FFFFFF;
	static constexpr idx_t AND_BUFFER_ID = 0x00000000FFFFFFFF;
	static constexpr idx_t AND_METADATA = 0xFF00000000000000;

public:
	//! Constructs an empty IndexPointer
	IndexPointer() : data(0) {};
	//! Constructs an in-memory IndexPointer with a buffer ID and an offset
	IndexPointer(const uint32_t buffer_id, const uint32_t offset) : data(0) {
		auto shifted_offset = ((idx_t)offset) << SHIFT_OFFSET;
		data += shifted_offset;
		data += buffer_id;
	};

public:
	//! Get data (all 64 bits)
	inline idx_t Get() const {
		return data;
	}
	//! Set data (all 64 bits)
	inline void Set(const idx_t data_p) {
		data = data_p;
	}

	//! Returns false, if the metadata is empty
	inline bool HasMetadata() const {
		return data & AND_METADATA;
	}
	//! Get metadata (zero to 7th bit)
	inline uint8_t GetMetadata() const {
		return data >> SHIFT_METADATA;
	}
	//! Set metadata (zero to 7th bit)
	inline void SetMetadata(const uint8_t metadata) {
		data += (idx_t)metadata << SHIFT_METADATA;
	}

	//! Get the offset (8th to 23rd bit)
	inline idx_t GetOffset() const {
		auto offset = data >> SHIFT_OFFSET;
		return offset & AND_OFFSET;
	}
	//! Get the buffer ID (24th to 63rd bit)
	inline idx_t GetBufferId() const {
		return data & AND_BUFFER_ID;
	}

	//! Resets the IndexPointer
	inline void Clear() {
		data = 0;
	}

	//! Adds an idx_t to a buffer ID, the rightmost 32 bits of data contain the buffer ID
	inline void IncreaseBufferId(const idx_t summand) {
		data += summand;
	}

	//! Comparison operator
	inline bool operator==(const IndexPointer &ptr) const {
		return data == ptr.data;
	}

private:
	//! Data holds all the information contained in an IndexPointer
	//! [0 - 7: metadata,
	//! 8 - 23: offset, 24 - 63: buffer ID]
	//! NOTE: we do not use bit fields because when using bit fields Windows compiles
	//! the IndexPointer class into 16 bytes instead of the intended 8 bytes, doubling the
	//! space requirements
	//! https://learn.microsoft.com/en-us/cpp/cpp/cpp-bit-fields?view=msvc-170
	idx_t data;
};

static_assert(sizeof(IndexPointer) == sizeof(idx_t), "Invalid size for IndexPointer.");

} // namespace duckdb
