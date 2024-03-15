//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/arrow_string_view_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

struct ArrowStringView {
public:
	static constexpr uint8_t MAX_INLINED_BYTES = 12 * sizeof(char);
	static constexpr uint8_t PREFIX_BYTES = 4 * sizeof(char);

public:
	ArrowStringView() = delete;
};

union arrow_string_view_t {
	arrow_string_view_t() {
	}

	//! Constructor for inlined arrow string views
	arrow_string_view_t(int32_t length, const char *data) {
		D_ASSERT(length <= ArrowStringView::MAX_INLINED_BYTES);
		inlined.length = length;
		memcpy(inlined.data, data, length);
		if (length < ArrowStringView::MAX_INLINED_BYTES) {
			// have to 0 pad
			uint8_t remaining_bytes = ArrowStringView::MAX_INLINED_BYTES - NumericCast<uint8_t>(length);

			memset(&inlined.data[length], '0', remaining_bytes);
		}
	}

	//! Constructor for non-inlined arrow string views
	arrow_string_view_t(int32_t length, const char *data, int32_t buffer_idx, int32_t offset) {
		D_ASSERT(length > ArrowStringView::MAX_INLINED_BYTES);
		ref.length = length;
		memcpy(ref.prefix, data, ArrowStringView::PREFIX_BYTES);
		ref.buffer_index = buffer_idx;
		ref.offset = offset;
	}

	//! Representation of inlined arrow string views
	struct {
		int32_t length;
		char data[ArrowStringView::MAX_INLINED_BYTES];
	} inlined;

	//! Representation of non-inlined arrow string views
	struct {
		int32_t length;
		char prefix[ArrowStringView::MAX_INLINED_BYTES];
		int32_t buffer_index, offset;
	} ref;

	int32_t Length() const {
		return inlined.length;
	}
	bool IsInline() const {
		return Length() <= ArrowStringView::MAX_INLINED_BYTES;
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
};

} // namespace duckdb
