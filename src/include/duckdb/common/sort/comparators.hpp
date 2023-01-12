//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/comparators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/row/row_layout.hpp"

namespace duckdb {

struct SortLayout;
struct SBScanState;

using ValidityBytes = RowLayout::ValidityBytes;

struct Comparators {
public:
	//! Whether a tie between two blobs can be broken
	static bool TieIsBreakable(const idx_t &col_idx, const data_ptr_t &row_ptr, const SortLayout &sort_layout);
	//! Compares the tuples that a being read from in the 'left' and 'right blocks during merge sort
	//! (only in case we cannot simply 'memcmp' - if there are blob columns)
	static int CompareTuple(const SBScanState &left, const SBScanState &right, const data_ptr_t &l_ptr,
	                        const data_ptr_t &r_ptr, const SortLayout &sort_layout, const bool &external_sort);
	//! Compare two blob values
	static int CompareVal(const data_ptr_t l_ptr, const data_ptr_t r_ptr, const LogicalType &type);

private:
	//! Compares two blob values that were initially tied by their prefix
	static int BreakBlobTie(const idx_t &tie_col, const SBScanState &left, const SBScanState &right,
	                        const SortLayout &sort_layout, const bool &external);
	//! Compare two fixed-size values
	template <class T>
	static int TemplatedCompareVal(const data_ptr_t &left_ptr, const data_ptr_t &right_ptr);

	//! Compare two values at the pointers (can be recursive if nested type)
	static int CompareValAndAdvance(data_ptr_t &l_ptr, data_ptr_t &r_ptr, const LogicalType &type, bool valid);
	//! Compares two fixed-size values at the given pointers
	template <class T>
	static int TemplatedCompareAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr);
	//! Compares two string values at the given pointers
	static int CompareStringAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, bool valid);
	//! Compares two struct values at the given pointers (recursive)
	static int CompareStructAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr,
	                                   const child_list_t<LogicalType> &types, bool valid);
	//! Compare two list values at the pointers (can be recursive if nested type)
	static int CompareListAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, const LogicalType &type, bool valid);
	//! Compares a list of fixed-size values
	template <class T>
	static int TemplatedCompareListLoop(data_ptr_t &left_ptr, data_ptr_t &right_ptr, const ValidityBytes &left_validity,
	                                    const ValidityBytes &right_validity, const idx_t &count);

	//! Unwizzles an offset into a pointer
	static void UnswizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type);
	//! Swizzles a pointer into an offset
	static void SwizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type);
};

} // namespace duckdb
