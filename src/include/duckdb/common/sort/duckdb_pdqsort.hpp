/*
pdqsort.h - Pattern-defeating quicksort.

Copyright (c) 2021 Orson Peters

This software is provided 'as-is', without any express or implied warranty. In no event will the
authors be held liable for any damages arising from the use of this software.

Permission is granted to anyone to use this software for any purpose, including commercial
applications, and to alter it and redistribute it freely, subject to the following restrictions:

1. The origin of this software must not be misrepresented; you must not claim that you wrote the
    original software. If you use this software in a product, an acknowledgment in the product
	documentation would be appreciated but is not required.

2. Altered source versions must be plainly marked as such, and must not be misrepresented as
    being the original software.

3. This notice may not be removed or altered from any source distribution.
*/

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <utility>

namespace duckdb_pdqsort {

using duckdb::idx_t;
using duckdb::data_t;
using duckdb::data_ptr_t;
using duckdb::unique_ptr;
using duckdb::unique_array;
using duckdb::unsafe_unique_array;
using duckdb::make_uniq_array;
using duckdb::make_unsafe_uniq_array;
using duckdb::FastMemcpy;
using duckdb::FastMemcmp;

// NOLINTBEGIN

enum {
	// Partitions below this size are sorted using insertion sort.
	insertion_sort_threshold = 24,

	// Partitions above this size use Tukey's ninther to select the pivot.
	ninther_threshold = 128,

	// When we detect an already sorted partition, attempt an insertion sort that allows this
	// amount of element moves before giving up.
	partial_insertion_sort_limit = 8,

	// Must be multiple of 8 due to loop unrolling, and < 256 to fit in unsigned char.
	block_size = 64,

	// Cacheline size, assumes power of two.
	cacheline_size = 64

};

// Returns floor(log2(n)), assumes n > 0.
template <class T>
inline int log2(T n) {
	int log = 0;
	while (n >>= 1) {
		++log;
	}
	return log;
}

struct PDQConstants {
	PDQConstants(idx_t entry_size, idx_t comp_offset, idx_t comp_size, data_ptr_t end)
	    : entry_size(entry_size), comp_offset(comp_offset), comp_size(comp_size),
	      tmp_buf_ptr(make_unsafe_uniq_array<data_t>(entry_size)), tmp_buf(tmp_buf_ptr.get()),
	      iter_swap_buf_ptr(make_unsafe_uniq_array<data_t>(entry_size)), iter_swap_buf(iter_swap_buf_ptr.get()),
	      swap_offsets_buf_ptr(make_unsafe_uniq_array<data_t>(entry_size)),
	      swap_offsets_buf(swap_offsets_buf_ptr.get()), end(end) {
	}

	const duckdb::idx_t entry_size;
	const idx_t comp_offset;
	const idx_t comp_size;

	unsafe_unique_array<data_t> tmp_buf_ptr;
	const data_ptr_t tmp_buf;

	unsafe_unique_array<data_t> iter_swap_buf_ptr;
	const data_ptr_t iter_swap_buf;

	unsafe_unique_array<data_t> swap_offsets_buf_ptr;
	const data_ptr_t swap_offsets_buf;

	const data_ptr_t end;
};

struct PDQIterator {
	PDQIterator(data_ptr_t ptr, const idx_t &entry_size) : ptr(ptr), entry_size(entry_size) {
	}

	inline PDQIterator(const PDQIterator &other) : ptr(other.ptr), entry_size(other.entry_size) {
	}

	inline const data_ptr_t &operator*() const {
		return ptr;
	}

	inline PDQIterator &operator++() {
		ptr += entry_size;
		return *this;
	}

	inline PDQIterator &operator--() {
		ptr -= entry_size;
		return *this;
	}

	inline PDQIterator operator++(int) {
		auto tmp = *this;
		ptr += entry_size;
		return tmp;
	}

	inline PDQIterator operator--(int) {
		auto tmp = *this;
		ptr -= entry_size;
		return tmp;
	}

	inline PDQIterator operator+(const idx_t &i) const {
		auto result = *this;
		result.ptr += i * entry_size;
		return result;
	}

	inline PDQIterator operator-(const idx_t &i) const {
		PDQIterator result = *this;
		result.ptr -= i * entry_size;
		return result;
	}

	inline PDQIterator &operator=(const PDQIterator &other) {
		D_ASSERT(entry_size == other.entry_size);
		ptr = other.ptr;
		return *this;
	}

	inline friend idx_t operator-(const PDQIterator &lhs, const PDQIterator &rhs) {
		D_ASSERT(duckdb::NumericCast<idx_t>(*lhs - *rhs) % lhs.entry_size == 0);
		D_ASSERT(*lhs - *rhs >= 0);
		return duckdb::NumericCast<idx_t>(*lhs - *rhs) / lhs.entry_size;
	}

	inline friend bool operator<(const PDQIterator &lhs, const PDQIterator &rhs) {
		return *lhs < *rhs;
	}

	inline friend bool operator>(const PDQIterator &lhs, const PDQIterator &rhs) {
		return *lhs > *rhs;
	}

	inline friend bool operator>=(const PDQIterator &lhs, const PDQIterator &rhs) {
		return *lhs >= *rhs;
	}

	inline friend bool operator<=(const PDQIterator &lhs, const PDQIterator &rhs) {
		return *lhs <= *rhs;
	}

	inline friend bool operator==(const PDQIterator &lhs, const PDQIterator &rhs) {
		return *lhs == *rhs;
	}

	inline friend bool operator!=(const PDQIterator &lhs, const PDQIterator &rhs) {
		return *lhs != *rhs;
	}

private:
	data_ptr_t ptr;
	const idx_t &entry_size;
};

static inline bool comp(const data_ptr_t &l, const data_ptr_t &r, const PDQConstants &constants) {
	D_ASSERT(l == constants.tmp_buf || l == constants.swap_offsets_buf || l < constants.end);
	D_ASSERT(r == constants.tmp_buf || r == constants.swap_offsets_buf || r < constants.end);
	return FastMemcmp(l + constants.comp_offset, r + constants.comp_offset, constants.comp_size) < 0;
}

static inline const data_ptr_t &GET_TMP(const data_ptr_t &src, const PDQConstants &constants) {
	D_ASSERT(src != constants.tmp_buf && src != constants.swap_offsets_buf && src < constants.end);
	FastMemcpy(constants.tmp_buf, src, constants.entry_size);
	return constants.tmp_buf;
}

static inline const data_ptr_t &SWAP_OFFSETS_GET_TMP(const data_ptr_t &src, const PDQConstants &constants) {
	D_ASSERT(src != constants.tmp_buf && src != constants.swap_offsets_buf && src < constants.end);
	FastMemcpy(constants.swap_offsets_buf, src, constants.entry_size);
	return constants.swap_offsets_buf;
}

static inline void MOVE(const data_ptr_t &dest, const data_ptr_t &src, const PDQConstants &constants) {
	D_ASSERT(dest == constants.tmp_buf || dest == constants.swap_offsets_buf || dest < constants.end);
	D_ASSERT(src == constants.tmp_buf || src == constants.swap_offsets_buf || src < constants.end);
	FastMemcpy(dest, src, constants.entry_size);
}

static inline void iter_swap(const PDQIterator &lhs, const PDQIterator &rhs, const PDQConstants &constants) {
	D_ASSERT(*lhs < constants.end);
	D_ASSERT(*rhs < constants.end);
	FastMemcpy(constants.iter_swap_buf, *lhs, constants.entry_size);
	FastMemcpy(*lhs, *rhs, constants.entry_size);
	FastMemcpy(*rhs, constants.iter_swap_buf, constants.entry_size);
}

// Sorts [begin, end) using insertion sort with the given comparison function.
inline void insertion_sort(const PDQIterator &begin, const PDQIterator &end, const PDQConstants &constants) {
	if (begin == end) {
		return;
	}

	for (PDQIterator cur = begin + 1; cur != end; ++cur) {
		PDQIterator sift = cur;
		PDQIterator sift_1 = cur - 1;

		// Compare first so we can avoid 2 moves for an element already positioned correctly.
		if (comp(*sift, *sift_1, constants)) {
			const auto &tmp = GET_TMP(*sift, constants);

			do {
				MOVE(*sift--, *sift_1, constants);
			} while (sift != begin && comp(tmp, *--sift_1, constants));

			MOVE(*sift, tmp, constants);
		}
	}
}

// Sorts [begin, end) using insertion sort with the given comparison function. Assumes
// *(begin - 1) is an element smaller than or equal to any element in [begin, end).
inline void unguarded_insertion_sort(const PDQIterator &begin, const PDQIterator &end, const PDQConstants &constants) {
	if (begin == end) {
		return;
	}

	for (PDQIterator cur = begin + 1; cur != end; ++cur) {
		PDQIterator sift = cur;
		PDQIterator sift_1 = cur - 1;

		// Compare first so we can avoid 2 moves for an element already positioned correctly.
		if (comp(*sift, *sift_1, constants)) {
			const auto &tmp = GET_TMP(*sift, constants);

			do {
				MOVE(*sift--, *sift_1, constants);
			} while (comp(tmp, *--sift_1, constants));

			MOVE(*sift, tmp, constants);
		}
	}
}

// Attempts to use insertion sort on [begin, end). Will return false if more than
// partial_insertion_sort_limit elements were moved, and abort sorting. Otherwise it will
// successfully sort and return true.
inline bool partial_insertion_sort(const PDQIterator &begin, const PDQIterator &end, const PDQConstants &constants) {
	if (begin == end) {
		return true;
	}

	std::size_t limit = 0;
	for (PDQIterator cur = begin + 1; cur != end; ++cur) {
		PDQIterator sift = cur;
		PDQIterator sift_1 = cur - 1;

		// Compare first so we can avoid 2 moves for an element already positioned correctly.
		if (comp(*sift, *sift_1, constants)) {
			const auto &tmp = GET_TMP(*sift, constants);

			do {
				MOVE(*sift--, *sift_1, constants);
			} while (sift != begin && comp(tmp, *--sift_1, constants));

			MOVE(*sift, tmp, constants);
			limit += cur - sift;
		}

		if (limit > partial_insertion_sort_limit) {
			return false;
		}
	}

	return true;
}

inline void sort2(const PDQIterator &a, const PDQIterator &b, const PDQConstants &constants) {
	if (comp(*b, *a, constants)) {
		iter_swap(a, b, constants);
	}
}

// Sorts the elements *a, *b and *c using comparison function comp.
inline void sort3(const PDQIterator &a, const PDQIterator &b, const PDQIterator &c, const PDQConstants &constants) {
	sort2(a, b, constants);
	sort2(b, c, constants);
	sort2(a, b, constants);
}

template <class T>
inline T *align_cacheline(T *p) {
#if defined(UINTPTR_MAX) && __cplusplus >= 201103L
	std::uintptr_t ip = reinterpret_cast<std::uintptr_t>(p);
#else
	std::size_t ip = reinterpret_cast<std::size_t>(p);
#endif
	ip = (ip + cacheline_size - 1) & -duckdb::UnsafeNumericCast<uintptr_t>(cacheline_size);
	return reinterpret_cast<T *>(ip);
}

inline void swap_offsets(const PDQIterator &first, const PDQIterator &last, unsigned char *offsets_l,
                         unsigned char *offsets_r, size_t num, bool use_swaps, const PDQConstants &constants) {
	if (use_swaps) {
		// This case is needed for the descending distribution, where we need
		// to have proper swapping for pdqsort to remain O(n).
		for (size_t i = 0; i < num; ++i) {
			iter_swap(first + offsets_l[i], last - offsets_r[i], constants);
		}
	} else if (num > 0) {
		PDQIterator l = first + offsets_l[0];
		PDQIterator r = last - offsets_r[0];
		const auto &tmp = SWAP_OFFSETS_GET_TMP(*l, constants);
		MOVE(*l, *r, constants);
		for (size_t i = 1; i < num; ++i) {
			l = first + offsets_l[i];
			MOVE(*r, *l, constants);
			r = last - offsets_r[i];
			MOVE(*l, *r, constants);
		}
		MOVE(*r, tmp, constants);
	}
}

// Partitions [begin, end) around pivot *begin using comparison function comp. Elements equal
// to the pivot are put in the right-hand partition. Returns the position of the pivot after
// partitioning and whether the passed sequence already was correctly partitioned. Assumes the
// pivot is a median of at least 3 elements and that [begin, end) is at least
// insertion_sort_threshold long. Uses branchless partitioning.
inline std::pair<PDQIterator, bool> partition_right_branchless(const PDQIterator &begin, const PDQIterator &end,
                                                               const PDQConstants &constants) {
	// Move pivot into local for speed.
	const auto &pivot = GET_TMP(*begin, constants);
	PDQIterator first = begin;
	PDQIterator last = end;

	// Find the first element greater than or equal than the pivot (the median of 3 guarantees
	// this exists).
	while (comp(*++first, pivot, constants)) {
	}

	// Find the first element strictly smaller than the pivot. We have to guard this search if
	// there was no element before *first.
	if (first - 1 == begin) {
		while (first < last && !comp(*--last, pivot, constants)) {
		}
	} else {
		while (!comp(*--last, pivot, constants)) {
		}
	}

	// If the first pair of elements that should be swapped to partition are the same element,
	// the passed in sequence already was correctly partitioned.
	bool already_partitioned = first >= last;
	if (!already_partitioned) {
		iter_swap(first, last, constants);
		++first;

		// The following branchless partitioning is derived from "BlockQuicksort: How Branch
		// Mispredictions don’t affect Quicksort" by Stefan Edelkamp and Armin Weiss, but
		// heavily micro-optimized.
		unsigned char offsets_l_storage[block_size + cacheline_size];
		unsigned char offsets_r_storage[block_size + cacheline_size];
		unsigned char *offsets_l = align_cacheline(offsets_l_storage);
		unsigned char *offsets_r = align_cacheline(offsets_r_storage);

		PDQIterator offsets_l_base = first;
		PDQIterator offsets_r_base = last;
		size_t num_l, num_r, start_l, start_r;
		num_l = num_r = start_l = start_r = 0;

		while (first < last) {
			// Fill up offset blocks with elements that are on the wrong side.
			// First we determine how much elements are considered for each offset block.
			size_t num_unknown = last - first;
			size_t left_split = num_l == 0 ? (num_r == 0 ? num_unknown / 2 : num_unknown) : 0;
			size_t right_split = num_r == 0 ? (num_unknown - left_split) : 0;

			// Fill the offset blocks.
			if (left_split >= block_size) {
				for (unsigned char i = 0; i < block_size;) {
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
				}
			} else {
				for (unsigned char i = 0; i < left_split;) {
					offsets_l[num_l] = i++;
					num_l += !comp(*first, pivot, constants);
					++first;
				}
			}

			if (right_split >= block_size) {
				for (unsigned char i = 0; i < block_size;) {
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
				}
			} else {
				for (unsigned char i = 0; i < right_split;) {
					offsets_r[num_r] = ++i;
					num_r += comp(*--last, pivot, constants);
				}
			}

			// Swap elements and update block sizes and first/last boundaries.
			size_t num = std::min(num_l, num_r);
			swap_offsets(offsets_l_base, offsets_r_base, offsets_l + start_l, offsets_r + start_r, num, num_l == num_r,
			             constants);
			num_l -= num;
			num_r -= num;
			start_l += num;
			start_r += num;

			if (num_l == 0) {
				start_l = 0;
				offsets_l_base = first;
			}

			if (num_r == 0) {
				start_r = 0;
				offsets_r_base = last;
			}
		}

		// We have now fully identified [first, last)'s proper position. Swap the last elements.
		if (num_l) {
			offsets_l += start_l;
			while (num_l--) {
				iter_swap(offsets_l_base + offsets_l[num_l], --last, constants);
			}
			first = last;
		}
		if (num_r) {
			offsets_r += start_r;
			while (num_r--) {
				iter_swap(offsets_r_base - offsets_r[num_r], first, constants), ++first;
			}
			last = first;
		}
	}

	// Put the pivot in the right place.
	PDQIterator pivot_pos = first - 1;
	MOVE(*begin, *pivot_pos, constants);
	MOVE(*pivot_pos, pivot, constants);

	return std::make_pair(pivot_pos, already_partitioned);
}

// Partitions [begin, end) around pivot *begin using comparison function comp. Elements equal
// to the pivot are put in the right-hand partition. Returns the position of the pivot after
// partitioning and whether the passed sequence already was correctly partitioned. Assumes the
// pivot is a median of at least 3 elements and that [begin, end) is at least
// insertion_sort_threshold long.
inline std::pair<PDQIterator, bool> partition_right(const PDQIterator &begin, const PDQIterator &end,
                                                    const PDQConstants &constants) {
	// Move pivot into local for speed.
	const auto &pivot = GET_TMP(*begin, constants);

	PDQIterator first = begin;
	PDQIterator last = end;

	// Find the first element greater than or equal than the pivot (the median of 3 guarantees
	// this exists).
	while (comp(*++first, pivot, constants)) {
	}

	// Find the first element strictly smaller than the pivot. We have to guard this search if
	// there was no element before *first.
	if (first - 1 == begin) {
		while (first < last && !comp(*--last, pivot, constants)) {
		}
	} else {
		while (!comp(*--last, pivot, constants)) {
		}
	}

	// If the first pair of elements that should be swapped to partition are the same element,
	// the passed in sequence already was correctly partitioned.
	bool already_partitioned = first >= last;

	// Keep swapping pairs of elements that are on the wrong side of the pivot. Previously
	// swapped pairs guard the searches, which is why the first iteration is special-cased
	// above.
	while (first < last) {
		iter_swap(first, last, constants);
		while (comp(*++first, pivot, constants)) {
		}
		while (!comp(*--last, pivot, constants)) {
		}
	}

	// Put the pivot in the right place.
	PDQIterator pivot_pos = first - 1;
	MOVE(*begin, *pivot_pos, constants);
	MOVE(*pivot_pos, pivot, constants);

	return std::make_pair(pivot_pos, already_partitioned);
}

// Similar function to the one above, except elements equal to the pivot are put to the left of
// the pivot and it doesn't check or return if the passed sequence already was partitioned.
// Since this is rarely used (the many equal case), and in that case pdqsort already has O(n)
// performance, no block quicksort is applied here for simplicity.
inline PDQIterator partition_left(const PDQIterator &begin, const PDQIterator &end, const PDQConstants &constants) {
	const auto &pivot = GET_TMP(*begin, constants);
	PDQIterator first = begin;
	PDQIterator last = end;

	while (comp(pivot, *--last, constants)) {
	}

	if (last + 1 == end) {
		while (first < last && !comp(pivot, *++first, constants)) {
		}
	} else {
		while (!comp(pivot, *++first, constants)) {
		}
	}

	while (first < last) {
		iter_swap(first, last, constants);
		while (comp(pivot, *--last, constants)) {
		}
		while (!comp(pivot, *++first, constants)) {
		}
	}

	PDQIterator pivot_pos = last;
	MOVE(*begin, *pivot_pos, constants);
	MOVE(*pivot_pos, pivot, constants);

	return pivot_pos;
}

template <bool Branchless>
inline void pdqsort_loop(PDQIterator begin, const PDQIterator &end, const PDQConstants &constants, int bad_allowed,
                         bool leftmost = true) {
	// Use a while loop for tail recursion elimination.
	while (true) {
		idx_t size = end - begin;

		// Insertion sort is faster for small arrays.
		if (size < insertion_sort_threshold) {
			if (leftmost) {
				insertion_sort(begin, end, constants);
			} else {
				unguarded_insertion_sort(begin, end, constants);
			}
			return;
		}

		// Choose pivot as median of 3 or pseudomedian of 9.
		idx_t s2 = size / 2;
		if (size > ninther_threshold) {
			sort3(begin, begin + s2, end - 1, constants);
			sort3(begin + 1, begin + (s2 - 1), end - 2, constants);
			sort3(begin + 2, begin + (s2 + 1), end - 3, constants);
			sort3(begin + (s2 - 1), begin + s2, begin + (s2 + 1), constants);
			iter_swap(begin, begin + s2, constants);
		} else {
			sort3(begin + s2, begin, end - 1, constants);
		}

		// If *(begin - 1) is the end of the right partition of a previous partition operation
		// there is no element in [begin, end) that is smaller than *(begin - 1). Then if our
		// pivot compares equal to *(begin - 1) we change strategy, putting equal elements in
		// the left partition, greater elements in the right partition. We do not have to
		// recurse on the left partition, since it's sorted (all equal).
		if (!leftmost && !comp(*(begin - 1), *begin, constants)) {
			begin = partition_left(begin, end, constants) + 1;
			continue;
		}

		// Partition and get results.
		std::pair<PDQIterator, bool> part_result =
		    Branchless ? partition_right_branchless(begin, end, constants) : partition_right(begin, end, constants);
		PDQIterator pivot_pos = part_result.first;
		bool already_partitioned = part_result.second;

		// Check for a highly unbalanced partition.
		idx_t l_size = pivot_pos - begin;
		idx_t r_size = end - (pivot_pos + 1);
		bool highly_unbalanced = l_size < size / 8 || r_size < size / 8;

		// If we got a highly unbalanced partition we shuffle elements to break many patterns.
		if (highly_unbalanced) {
			// If we had too many bad partitions, switch to heapsort to guarantee O(n log n).
			//			if (--bad_allowed == 0) {
			//				std::make_heap(begin, end, comp);
			//				std::sort_heap(begin, end, comp);
			//				return;
			//			}

			if (l_size >= insertion_sort_threshold) {
				iter_swap(begin, begin + l_size / 4, constants);
				iter_swap(pivot_pos - 1, pivot_pos - l_size / 4, constants);

				if (l_size > ninther_threshold) {
					iter_swap(begin + 1, begin + (l_size / 4 + 1), constants);
					iter_swap(begin + 2, begin + (l_size / 4 + 2), constants);
					iter_swap(pivot_pos - 2, pivot_pos - (l_size / 4 + 1), constants);
					iter_swap(pivot_pos - 3, pivot_pos - (l_size / 4 + 2), constants);
				}
			}

			if (r_size >= insertion_sort_threshold) {
				iter_swap(pivot_pos + 1, pivot_pos + (1 + r_size / 4), constants);
				iter_swap(end - 1, end - r_size / 4, constants);

				if (r_size > ninther_threshold) {
					iter_swap(pivot_pos + 2, pivot_pos + (2 + r_size / 4), constants);
					iter_swap(pivot_pos + 3, pivot_pos + (3 + r_size / 4), constants);
					iter_swap(end - 2, end - (1 + r_size / 4), constants);
					iter_swap(end - 3, end - (2 + r_size / 4), constants);
				}
			}
		} else {
			// If we were decently balanced and we tried to sort an already partitioned
			// sequence try to use insertion sort.
			if (already_partitioned && partial_insertion_sort(begin, pivot_pos, constants) &&
			    partial_insertion_sort(pivot_pos + 1, end, constants)) {
				return;
			}
		}

		// Sort the left partition first using recursion and do tail recursion elimination for
		// the right-hand partition.
		pdqsort_loop<Branchless>(begin, pivot_pos, constants, bad_allowed, leftmost);
		begin = pivot_pos + 1;
		leftmost = false;
	}
}

inline void pdqsort(const PDQIterator &begin, const PDQIterator &end, const PDQConstants &constants) {
	if (begin == end) {
		return;
	}
	pdqsort_loop<false>(begin, end, constants, log2(end - begin));
}

inline void pdqsort_branchless(const PDQIterator &begin, const PDQIterator &end, const PDQConstants &constants) {
	if (begin == end) {
		return;
	}
	pdqsort_loop<true>(begin, end, constants, log2(end - begin));
}
// NOLINTEND

} // namespace duckdb_pdqsort
