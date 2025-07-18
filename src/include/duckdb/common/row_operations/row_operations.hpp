//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/row_operations/row_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class ArenaAllocator;
struct AggregateObject;
struct AggregateFilterData;
class DataChunk;
class RowLayout;
class TupleDataLayout;
class RowDataCollection;
struct SelectionVector;
class StringHeap;
struct UnifiedVectorFormat;

// The NestedValidity class help to set/get the validity from inside nested vectors
class NestedValidity {
	data_ptr_t list_validity_location;
	data_ptr_t *struct_validity_locations;
	idx_t entry_idx;
	idx_t idx_in_entry;
	idx_t list_validity_offset;

public:
	explicit NestedValidity(data_ptr_t validitymask_location);
	NestedValidity(data_ptr_t *validitymask_locations, idx_t child_vector_index);
	void SetInvalid(idx_t idx);
	bool IsValid(idx_t idx);
	void OffsetListBy(idx_t offset);
};

struct RowOperationsState {
	explicit RowOperationsState(ArenaAllocator &allocator) : allocator(allocator) {
	}

	ArenaAllocator &allocator;
	unique_ptr<Vector> addresses; // Re-usable vector for row_aggregate.cpp
};

// RowOperations contains a set of operations that operate on data using a RowLayout
struct RowOperations {
	//===--------------------------------------------------------------------===//
	// Aggregation Operators
	//===--------------------------------------------------------------------===//
	//! initialize - unaligned addresses
	static void InitializeStates(TupleDataLayout &layout, Vector &addresses, const SelectionVector &sel, idx_t count);
	//! destructor - unaligned addresses, updated
	static void DestroyStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses, idx_t count);
	//! update - aligned addresses
	static void UpdateStates(RowOperationsState &state, AggregateObject &aggr, Vector &addresses, DataChunk &payload,
	                         idx_t arg_idx, idx_t count);
	//! filtered update - aligned addresses
	static void UpdateFilteredStates(RowOperationsState &state, AggregateFilterData &filter_data, AggregateObject &aggr,
	                                 Vector &addresses, DataChunk &payload, idx_t arg_idx);
	//! combine - unaligned addresses, updated
	static void CombineStates(RowOperationsState &state, TupleDataLayout &layout, Vector &sources, Vector &targets,
	                          idx_t count);
	//! finalize - unaligned addresses, updated
	static void FinalizeStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses, DataChunk &result,
	                           idx_t aggr_idx);

	//===--------------------------------------------------------------------===//
	// Read/Write Operators
	//===--------------------------------------------------------------------===//
	//! Scatter group data to the rows. Initialises the ValidityMask.
	static void Scatter(DataChunk &columns, UnifiedVectorFormat col_data[], const RowLayout &layout, Vector &rows,
	                    RowDataCollection &string_heap, const SelectionVector &sel, idx_t count);
	//! Gather a single column.
	//! If heap_ptr is not null, then the data is assumed to contain swizzled pointers,
	//! which will be unswizzled in memory.
	static void Gather(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
	                   const idx_t count, const RowLayout &layout, const idx_t col_no, const idx_t build_size = 0,
	                   data_ptr_t heap_ptr = nullptr);

	//===--------------------------------------------------------------------===//
	// Heap Operators
	//===--------------------------------------------------------------------===//
	//! Compute the entry sizes of a vector with variable size type (used before building heap buffer space).
	static void ComputeEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
	                              const SelectionVector &sel, idx_t offset = 0);
	//! Compute the entry sizes of vector data with variable size type (used before building heap buffer space).
	static void ComputeEntrySizes(Vector &v, UnifiedVectorFormat &vdata, idx_t entry_sizes[], idx_t vcount,
	                              idx_t ser_count, const SelectionVector &sel, idx_t offset = 0);
	//! Scatter vector with variable size type to the heap.
	static void HeapScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                        data_ptr_t *key_locations, optional_ptr<NestedValidity> parent_validity, idx_t offset = 0);
	//! Scatter vector data with variable size type to the heap.
	static void HeapScatterVData(UnifiedVectorFormat &vdata, PhysicalType type, const SelectionVector &sel,
	                             idx_t ser_count, data_ptr_t *key_locations,
	                             optional_ptr<NestedValidity> parent_validity, idx_t offset = 0);
	//! Gather a single column with variable size type from the heap.
	static void HeapGather(Vector &v, const idx_t &vcount, const SelectionVector &sel, data_ptr_t key_locations[],
	                       optional_ptr<NestedValidity> parent_validity);

	//===--------------------------------------------------------------------===//
	// Sorting Operators
	//===--------------------------------------------------------------------===//
	//! Scatter vector data to the rows in radix-sortable format.
	static void RadixScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                         data_ptr_t key_locations[], bool desc, bool has_null, bool nulls_first, idx_t prefix_len,
	                         idx_t width, idx_t offset = 0);

	//===--------------------------------------------------------------------===//
	// Out-of-Core Operators
	//===--------------------------------------------------------------------===//
	//! Swizzles blob pointers to offset within heap row
	static void SwizzleColumns(const RowLayout &layout, const data_ptr_t base_row_ptr, const idx_t count);
	//! Swizzles the base pointer of each row to offset within heap block
	static void SwizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
	                               const idx_t count, const idx_t base_offset = 0);
	//! Copies 'count' heap rows that are pointed to by the rows at 'row_ptr' to 'heap_ptr' and swizzles the pointers
	static void CopyHeapAndSwizzle(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
	                               data_ptr_t heap_ptr, const idx_t count);

	//! Unswizzles the base offset within heap block the rows to pointers
	static void UnswizzleHeapPointer(const RowLayout &layout, const data_ptr_t base_row_ptr,
	                                 const data_ptr_t base_heap_ptr, const idx_t count);
	//! Unswizzles all offsets back to pointers
	static void UnswizzlePointers(const RowLayout &layout, const data_ptr_t base_row_ptr,
	                              const data_ptr_t base_heap_ptr, const idx_t count);
};

} // namespace duckdb
