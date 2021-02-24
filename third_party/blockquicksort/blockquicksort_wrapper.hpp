//===----------------------------------------------------------------------===//
//                         DuckDB
//
// blockquicksort_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "partition.h"
#include "quicksort.h"

namespace duckdb {

class BlockQuickSort {
public:
	template <class RANDOM_ACCESS_ITERATOR>
	static void Sort(RANDOM_ACCESS_ITERATOR begin, RANDOM_ACCESS_ITERATOR end) {
        quicksort::qsort<partition::Hoare_block_partition_mosqrt>(begin, end);
	}

    template <class RANDOM_ACCESS_ITERATOR, class COMPARE>
	static void Sort(RANDOM_ACCESS_ITERATOR begin, RANDOM_ACCESS_ITERATOR end, COMPARE less) {
        quicksort::qsort<partition::Hoare_block_partition_mosqrt>(begin, end, less);
	}
};

} // namespace duckdb
