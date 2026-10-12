//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/buffered_merge.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Merges the sorted ranges [first, middle) and [middle, last) in place using the given scratch buffer, which must
//! hold at least min(middle - first, last - middle) keys. The smaller side is moved into the buffer and merged back,
//! forward when it is the left side and backward when it is the right side, which keeps equal keys in order.
template <class SORT_KEY, class ITERATOR, class COMPARE>
void BufferedMerge(ITERATOR first, ITERATOR middle, ITERATOR last, const COMPARE &compare, SORT_KEY *buffer,
                   const idx_t buffer_count) {
	const auto left_count = static_cast<idx_t>(middle - first);
	const auto right_count = static_cast<idx_t>(last - middle);
	if (left_count == 0 || right_count == 0) {
		return;
	}
	if (MinValue(left_count, right_count) > buffer_count) {
		std::inplace_merge(first, middle, last, compare);
		return;
	}
	if (left_count <= right_count) {
		// move the left side out and merge forward into [first, last)
		std::move(first, middle, buffer);
		auto buffer_end = buffer + left_count;
		auto buffer_it = buffer;
		auto right_it = middle;
		auto out = first;
		while (buffer_it != buffer_end && right_it != last) {
			if (compare(*right_it, *buffer_it)) {
				*out = std::move(*right_it);
				++right_it;
			} else {
				*out = std::move(*buffer_it);
				++buffer_it;
			}
			++out;
		}
		std::move(buffer_it, buffer_end, out);
		return;
	}
	// move the right side out and merge backward into [first, last)
	std::move(middle, last, buffer);
	auto buffer_begin = buffer;
	auto buffer_it = buffer + right_count;
	auto left_it = middle;
	auto out = last;
	while (buffer_it != buffer_begin && left_it != first) {
		--out;
		if (compare(*(buffer_it - 1), *(left_it - 1))) {
			--left_it;
			*out = std::move(*left_it);
		} else {
			--buffer_it;
			*out = std::move(*buffer_it);
		}
	}
	// anything left in the buffer goes to the front of the remaining gap
	std::move_backward(buffer_begin, buffer_it, out);
}

} // namespace duckdb
