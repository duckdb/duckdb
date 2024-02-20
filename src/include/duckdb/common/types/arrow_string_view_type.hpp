//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/arrow_string_view_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

union arrow_string_view_t {
	arrow_string_view_t() {
	}

	//! Constructor for inlined arrow string views
	arrow_string_view_t(int32_t length, const char *data) {
		D_ASSERT(length <= max_inlined_bytes);
		inlined.length = length;
		memcpy(inlined.data, data, length);
		if (length < max_inlined_bytes) {
			// have to 0 pad
			uint8_t remaining_bytes = max_inlined_bytes - length;
			memset(&inlined.data[length], '0', remaining_bytes);
		}
	}

	//! Constructor for non-inlined arrow string views
	arrow_string_view_t(int32_t length, const char *data, int32_t buffer_idx, int32_t offset) {
		D_ASSERT(length > max_inlined_bytes);
		ref.length = length;
		memcpy(ref.prefix, data, 4);
		ref.buffer_index = buffer_idx;
		ref.offset = offset;
	}

	//! Representation of inlined arrow string views
	struct {
		int32_t length;
		char data[12];
	} inlined;

	//! Representation of non-inlined arrow string views
	struct {
		int32_t length;
		char prefix[4];
		int32_t buffer_index, offset;
	} ref;

	int32_t Length() const {
		return inlined.length;
	}
	bool IsInline() const {
		return Length() <= max_inlined_bytes;
	}

	const char *GetInlineData() const {
		return IsInline() ? inlined.data : ref.prefix;
	}
	int32_t GetBufferIndex() {
		D_ASSERT(!IsInline());
		return ref.buffer_index;
	}
	int32_t GetOffset() {
		D_ASSERT(!IsInline());
		return ref.offset;
	}
	static constexpr uint8_t max_inlined_bytes = 12;
};
