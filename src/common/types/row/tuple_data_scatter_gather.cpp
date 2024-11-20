#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/uhugeint.hpp"

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

template <class T>
static constexpr idx_t TupleDataWithinListFixedSize() {
	return sizeof(T);
}

template <>
constexpr idx_t TupleDataWithinListFixedSize<string_t>() {
	return sizeof(uint32_t);
}

template <class T>
static void TupleDataValueStore(const T &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                data_ptr_t &) {
	Store<T>(source, row_location + offset_in_row);
}

template <>
inline void TupleDataValueStore(const string_t &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                data_ptr_t &heap_location) {
#ifdef DEBUG
	source.VerifyCharacters();
#endif
	if (source.IsInlined()) {
		Store<string_t>(source, row_location + offset_in_row);
	} else {
		FastMemcpy(heap_location, source.GetData(), source.GetSize());
		Store<string_t>(string_t(const_char_ptr_cast(heap_location), UnsafeNumericCast<uint32_t>(source.GetSize())),
		                row_location + offset_in_row);
		heap_location += source.GetSize();
	}
}

template <class T>
static void TupleDataWithinListValueStore(const T &source, const data_ptr_t &location, data_ptr_t &) {
	Store<T>(source, location);
}

template <>
inline void TupleDataWithinListValueStore(const string_t &source, const data_ptr_t &location,
                                          data_ptr_t &heap_location) {
#ifdef DEBUG
	source.VerifyCharacters();
#endif
	Store<uint32_t>(UnsafeNumericCast<uint32_t>(source.GetSize()), location);
	FastMemcpy(heap_location, source.GetData(), source.GetSize());
	heap_location += source.GetSize();
}

template <class T>
void TupleDataValueVerify(const LogicalType &, const T &) {
#ifdef DEBUG
	// NOP
#endif
}

template <>
inline void TupleDataValueVerify(const LogicalType &type, const string_t &value) {
#ifdef DEBUG
	if (type.id() == LogicalTypeId::VARCHAR) {
		value.Verify();
	}
#endif
}

template <class T>
static T TupleDataWithinListValueLoad(const data_ptr_t &location, data_ptr_t &) {
	return Load<T>(location);
}

template <>
inline string_t TupleDataWithinListValueLoad(const data_ptr_t &location, data_ptr_t &heap_location) {
	const auto size = Load<uint32_t>(location);
	string_t result(const_char_ptr_cast(heap_location), size);
	heap_location += size;
	return result;
}

static void ResetCombinedListData(vector<TupleDataVectorFormat> &vector_data) {
#ifdef DEBUG
	for (auto &vd : vector_data) {
		vd.combined_list_data = nullptr;
		ResetCombinedListData(vd.children);
	}
#endif
}

void TupleDataCollection::ComputeHeapSizes(TupleDataChunkState &chunk_state, const DataChunk &new_chunk,
                                           const SelectionVector &append_sel, const idx_t append_count) {
	ResetCombinedListData(chunk_state.vector_data);

	auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
	std::fill_n(heap_sizes, append_count, 0);

	for (idx_t col_idx = 0; col_idx < new_chunk.ColumnCount(); col_idx++) {
		auto &source_v = new_chunk.data[col_idx];
		auto &source_format = chunk_state.vector_data[col_idx];
		ComputeHeapSizes(chunk_state.heap_sizes, source_v, source_format, append_sel, append_count);
	}
}

static idx_t StringHeapSize(const string_t &val) {
	return val.IsInlined() ? 0 : val.GetSize();
}

void TupleDataCollection::ComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                           TupleDataVectorFormat &source_format, const SelectionVector &append_sel,
                                           const idx_t append_count) {
	const auto type = source_v.GetType().InternalType();
	if (type != PhysicalType::VARCHAR && type != PhysicalType::STRUCT && type != PhysicalType::LIST &&
	    type != PhysicalType::ARRAY) {
		return;
	}

	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	// Source
	const auto &source_vector_data = source_format.unified;
	const auto &source_sel = *source_vector_data.sel;
	const auto &source_validity = source_vector_data.validity;

	switch (type) {
	case PhysicalType::VARCHAR: {
		// Only non-inlined strings are stored in the heap
		const auto source_data = UnifiedVectorFormat::GetData<string_t>(source_vector_data);
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (source_validity.RowIsValid(source_idx)) {
				heap_sizes[i] += StringHeapSize(source_data[source_idx]);
			} else {
				heap_sizes[i] += StringHeapSize(NullValue<string_t>());
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		// Recurse through the struct children
		auto &struct_sources = StructVector::GetEntries(source_v);
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
			const auto &struct_source = struct_sources[struct_col_idx];
			auto &struct_format = source_format.children[struct_col_idx];
			ComputeHeapSizes(heap_sizes_v, *struct_source, struct_format, append_sel, append_count);
		}
		break;
	}
	case PhysicalType::LIST: {
		// Lists are stored entirely in the heap
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (source_validity.RowIsValid(source_idx)) {
				heap_sizes[i] += sizeof(uint64_t); // Size of the list
			}
		}

		// Recurse
		D_ASSERT(source_format.children.size() == 1);
		auto &child_source_v = ListVector::GetEntry(source_v);
		auto &child_format = source_format.children[0];
		WithinCollectionComputeHeapSizes(heap_sizes_v, child_source_v, child_format, append_sel, append_count,
		                                 source_vector_data);
		break;
	}
	case PhysicalType::ARRAY: {
		// Arrays are stored entirely in the heap
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (source_validity.RowIsValid(source_idx)) {
				heap_sizes[i] += sizeof(uint64_t); // Size of the list
			}
		}

		// Recurse
		D_ASSERT(source_format.children.size() == 1);
		auto &child_source_v = ArrayVector::GetEntry(source_v);
		auto &child_format = source_format.children[0];
		WithinCollectionComputeHeapSizes(heap_sizes_v, child_source_v, child_format, append_sel, append_count,
		                                 source_vector_data);
		break;
	}
	default:
		throw NotImplementedException("ComputeHeapSizes for %s", EnumUtil::ToString(source_v.GetType().id()));
	}
}

void TupleDataCollection::WithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                           TupleDataVectorFormat &source_format,
                                                           const SelectionVector &append_sel, const idx_t append_count,
                                                           const UnifiedVectorFormat &list_data) {
	auto type = source_v.GetType().InternalType();
	if (TypeIsConstantSize(type)) {
		ComputeFixedWithinCollectionHeapSizes(heap_sizes_v, source_v, source_format, append_sel, append_count,
		                                      list_data);
		return;
	}
	switch (type) {
	case PhysicalType::VARCHAR:
		StringWithinCollectionComputeHeapSizes(heap_sizes_v, source_v, source_format, append_sel, append_count,
		                                       list_data);
		break;
	case PhysicalType::STRUCT:
		StructWithinCollectionComputeHeapSizes(heap_sizes_v, source_v, source_format, append_sel, append_count,
		                                       list_data);
		break;
	case PhysicalType::LIST:
		CollectionWithinCollectionComputeHeapSizes(heap_sizes_v, source_v, source_format, append_sel, append_count,
		                                           list_data);
		break;
	case PhysicalType::ARRAY:
		CollectionWithinCollectionComputeHeapSizes(heap_sizes_v, source_v, source_format, append_sel, append_count,
		                                           list_data);
		break;
	default:
		throw NotImplementedException("WithinListHeapComputeSizes for %s", EnumUtil::ToString(source_v.GetType().id()));
	}
}

void TupleDataCollection::ComputeFixedWithinCollectionHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                                TupleDataVectorFormat &,
                                                                const SelectionVector &append_sel,
                                                                const idx_t append_count,
                                                                const UnifiedVectorFormat &list_data) {
	// Parent list data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	D_ASSERT(TypeIsConstantSize(source_v.GetType().InternalType()));
	const auto type_size = GetTypeIdSize(source_v.GetType().InternalType());
	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list length
		const auto &list_length = list_entries[list_idx].length;
		if (list_length == 0) {
			continue;
		}

		// Size is validity mask and all values
		auto &heap_size = heap_sizes[i];
		heap_size += ValidityBytes::SizeInBytes(list_length);
		heap_size += list_length * type_size;
	}
}

void TupleDataCollection::StringWithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &,
                                                                 TupleDataVectorFormat &source_format,
                                                                 const SelectionVector &append_sel,
                                                                 const idx_t append_count,
                                                                 const UnifiedVectorFormat &list_data) {
	// Parent list data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<string_t>(source_data);
	const auto &source_validity = source_data.validity;

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;
		if (list_length == 0) {
			continue;
		}

		// Size is validity mask and all string sizes
		auto &heap_size = heap_sizes[i];
		heap_size += ValidityBytes::SizeInBytes(list_length);
		heap_size += list_length * TupleDataWithinListFixedSize<string_t>();

		// Plus all the actual strings
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_source_idx = source_sel.get_index(list_offset + child_i);
			if (source_validity.RowIsValid(child_source_idx)) {
				heap_size += data[child_source_idx].GetSize();
			}
		}
	}
}

void TupleDataCollection::StructWithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                                 TupleDataVectorFormat &source_format,
                                                                 const SelectionVector &append_sel,
                                                                 const idx_t append_count,
                                                                 const UnifiedVectorFormat &list_data) {
	// Parent list data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list length
		const auto &list_length = list_entries[list_idx].length;
		if (list_length == 0) {
			continue;
		}

		// Size is just the validity mask
		heap_sizes[i] += ValidityBytes::SizeInBytes(list_length);
	}

	// Recurse
	auto &struct_sources = StructVector::GetEntries(source_v);
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
		auto &struct_source = *struct_sources[struct_col_idx];

		auto &struct_format = source_format.children[struct_col_idx];
		WithinCollectionComputeHeapSizes(heap_sizes_v, struct_source, struct_format, append_sel, append_count,
		                                 list_data);
	}
}

static void ApplySliceRecursive(const Vector &source_v, TupleDataVectorFormat &source_format,
                                const SelectionVector &combined_sel, const idx_t count) {
	D_ASSERT(source_format.combined_list_data);
	auto &combined_list_data = *source_format.combined_list_data;

	combined_list_data.selection_data = source_format.original_sel->Slice(combined_sel, count);
	source_format.unified.owned_sel.Initialize(combined_list_data.selection_data);
	source_format.unified.sel = &source_format.unified.owned_sel;

	if (source_v.GetType().InternalType() == PhysicalType::STRUCT) {
		// We have to apply it to the child vectors too
		auto &struct_sources = StructVector::GetEntries(source_v);
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
			auto &struct_source = *struct_sources[struct_col_idx];
			auto &struct_format = source_format.children[struct_col_idx];
#ifdef DEBUG
			D_ASSERT(!struct_format.combined_list_data);
#endif
			if (!struct_format.combined_list_data) {
				struct_format.combined_list_data = make_uniq<CombinedListData>();
			}
			ApplySliceRecursive(struct_source, struct_format, *source_format.unified.sel, count);
		}
	}
}

void TupleDataCollection::CollectionWithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                                     TupleDataVectorFormat &source_format,
                                                                     const SelectionVector &append_sel,
                                                                     const idx_t append_count,
                                                                     const UnifiedVectorFormat &list_data) {
	// Parent list data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Source
	const auto &child_list_data = source_format.unified;
	const auto child_list_sel = *child_list_data.sel;
	const auto child_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(child_list_data);
	const auto &child_list_validity = child_list_data.validity;

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	// Figure out actual child list size (can differ from ListVector::GetListSize if dict/const vector),
	// and we cannot use ConstantVector::ZeroSelectionVector because it may need to be longer than STANDARD_VECTOR_SIZE
	idx_t sum_of_sizes = 0;
	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue;
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;
		if (list_length == 0) {
			continue;
		}

		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_list_idx = child_list_sel.get_index(list_offset + child_i);
			if (!child_list_validity.RowIsValid(child_list_idx)) {
				continue;
			}

			const auto &child_list_entry = child_list_entries[child_list_idx];
			const auto &child_list_length = child_list_entry.length;

			sum_of_sizes += child_list_length;
		}
	}

	const auto child_list_child_count = MaxValue<idx_t>(
	    sum_of_sizes, source_v.GetType().InternalType() == PhysicalType::LIST ? ListVector::GetListSize(source_v)
	                                                                          : ArrayVector::GetTotalSize(source_v));

	D_ASSERT(source_format.children.size() == 1);
	auto &child_format = source_format.children[0];
#ifdef DEBUG
	// In debug mode this should be deleted by ResetCombinedListData
	D_ASSERT(!child_format.combined_list_data);
#endif
	if (!child_format.combined_list_data) {
		child_format.combined_list_data = make_uniq<CombinedListData>();
	}
	auto &combined_list_data = *child_format.combined_list_data;

	// Construct combined list entries and a selection/validity vector for the child list child
	SelectionVector combined_sel(child_list_child_count);
	for (idx_t i = 0; i < child_list_child_count; i++) {
		combined_sel.set_index(i, 0);
	}
	auto &combined_list_entries = combined_list_data.combined_list_entries;
	auto &combined_validity = combined_list_data.combined_validity;
	combined_validity.SetAllValid(STANDARD_VECTOR_SIZE);

	idx_t combined_list_offset = 0;
	for (idx_t i = 0; i < append_count; i++) {
		const auto append_idx = append_sel.get_index(i);
		const auto list_idx = list_sel.get_index(append_idx);
		if (!list_validity.RowIsValid(list_idx)) {
			combined_validity.SetInvalidUnsafe(append_idx);
			continue; // Original list entry is invalid - no need to serialize the child list
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Size is the validity mask and the list sizes
		auto &heap_size = heap_sizes[i];
		heap_size += ValidityBytes::SizeInBytes(list_length);
		heap_size += list_length * sizeof(uint64_t);

		idx_t child_list_size = 0;
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_list_idx = child_list_sel.get_index(list_offset + child_i);
			if (child_list_validity.RowIsValid(child_list_idx)) {
				const auto &child_list_entry = child_list_entries[child_list_idx];
				const auto &child_list_offset = child_list_entry.offset;
				const auto &child_list_length = child_list_entry.length;
				if (child_list_length == 0) {
					continue;
				}

				// Add this child's list entries to the combined selection vector
				for (idx_t child_value_i = 0; child_value_i < child_list_length; child_value_i++) {
					auto idx = combined_list_offset + child_list_size + child_value_i;
					auto loc = child_list_offset + child_value_i;
					combined_sel.set_index(idx, loc);
				}

				child_list_size += child_list_length;
			}
		}

		// Combine the child list entries into one
		auto &combined_list_entry = combined_list_entries[append_idx];
		combined_list_entry.offset = combined_list_offset;
		combined_list_entry.length = child_list_size;
		combined_list_offset += child_list_size;
	}

	// TODO: Template this?
	auto &child_source = source_v.GetType().InternalType() == PhysicalType::LIST ? ListVector::GetEntry(source_v)
	                                                                             : ArrayVector::GetEntry(source_v);
	ApplySliceRecursive(child_source, child_format, combined_sel, child_list_child_count);

	// Create a combined child_list_data to be used as list_data in the recursion
	auto &combined_child_list_data = combined_list_data.combined_data;
	combined_child_list_data.sel = FlatVector::IncrementalSelectionVector();
	combined_child_list_data.data = data_ptr_cast(combined_list_entries);
	combined_child_list_data.validity.Initialize(combined_validity);

	// Recurse
	WithinCollectionComputeHeapSizes(heap_sizes_v, child_source, child_format, append_sel, append_count,
	                                 combined_child_list_data);
}

template <class T>
static void TemplatedInitializeValidityMask(const data_ptr_t row_locations[], const idx_t append_count) {
	for (idx_t i = 0; i < append_count; i++) {
		Store<T>(T(-1), row_locations[i]);
	}
}

template <idx_t validity_bytes>
static void TemplatedInitializeValidityMask(const data_ptr_t row_locations[], const idx_t append_count) {
	for (idx_t i = 0; i < append_count; i++) {
		memset(row_locations[i], ~0, validity_bytes);
	}
}

static void InitializeValidityMask(const data_ptr_t row_locations[], const idx_t append_count,
                                   const idx_t validity_bytes) {
	switch (validity_bytes) {
	case 1:
		TemplatedInitializeValidityMask<uint8_t>(row_locations, append_count);
		break;
	case 2:
		TemplatedInitializeValidityMask<uint16_t>(row_locations, append_count);
		break;
	case 3:
		TemplatedInitializeValidityMask<3>(row_locations, append_count);
		break;
	case 4:
		TemplatedInitializeValidityMask<uint32_t>(row_locations, append_count);
		break;
	case 5:
		TemplatedInitializeValidityMask<5>(row_locations, append_count);
		break;
	case 6:
		TemplatedInitializeValidityMask<6>(row_locations, append_count);
		break;
	case 7:
		TemplatedInitializeValidityMask<7>(row_locations, append_count);
		break;
	case 8:
		TemplatedInitializeValidityMask<uint64_t>(row_locations, append_count);
		break;
	default:
		for (idx_t i = 0; i < append_count; i++) {
			FastMemset(row_locations[i], ~0, validity_bytes);
		}
	}
}

void TupleDataCollection::Scatter(TupleDataChunkState &chunk_state, const DataChunk &new_chunk,
                                  const SelectionVector &append_sel, const idx_t append_count) const {
#ifdef DEBUG
	Vector heap_locations_copy(LogicalType::POINTER);
	if (!layout.AllConstant()) {
		const auto heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);
		const auto copied_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations_copy);
		for (idx_t i = 0; i < append_count; i++) {
			copied_heap_locations[i] = heap_locations[i];
		}
	}
#endif

	const auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);

	// Set the validity mask for each row before inserting data
	InitializeValidityMask(row_locations, append_count, ValidityBytes::SizeInBytes(layout.ColumnCount()));

	if (!layout.AllConstant()) {
		// Set the heap size for each row
		const auto heap_size_offset = layout.GetHeapSizeOffset();
		const auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
		for (idx_t i = 0; i < append_count; i++) {
			Store<uint32_t>(UnsafeNumericCast<uint32_t>(heap_sizes[i]), row_locations[i] + heap_size_offset);
		}
	}

	// Write the data
	for (const auto &col_idx : chunk_state.column_ids) {
		Scatter(chunk_state, new_chunk.data[col_idx], col_idx, append_sel, append_count);
	}

#ifdef DEBUG
	// Verify that the size of the data written to the heap is the same as the size we computed it would be
	if (!layout.AllConstant()) {
		const auto original_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations_copy);
		const auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
		const auto offset_heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);
		for (idx_t i = 0; i < append_count; i++) {
			if (heap_sizes[i] != 0) {
				D_ASSERT(offset_heap_locations[i] == original_heap_locations[i] + heap_sizes[i]);
			}
		}
	}
#endif
}

void TupleDataCollection::Scatter(TupleDataChunkState &chunk_state, const Vector &source, const column_t column_id,
                                  const SelectionVector &append_sel, const idx_t append_count) const {
	const auto &scatter_function = scatter_functions[column_id];
	scatter_function.function(source, chunk_state.vector_data[column_id], append_sel, append_count, layout,
	                          chunk_state.row_locations, chunk_state.heap_locations, column_id,
	                          chunk_state.vector_data[column_id].unified, scatter_function.child_functions);
}

template <class T>
static void TupleDataTemplatedScatter(const Vector &, const TupleDataVectorFormat &source_format,
                                      const SelectionVector &append_sel, const idx_t append_count,
                                      const TupleDataLayout &layout, const Vector &row_locations,
                                      Vector &heap_locations, const idx_t col_idx, const UnifiedVectorFormat &,
                                      const vector<TupleDataScatterFunction> &) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<T>(source_data);
	const auto &validity = source_data.validity;

	// Target
	const auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	if (validity.AllValid()) {
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			TupleDataValueStore<T>(data[source_idx], target_locations[i], offset_in_row, target_heap_locations[i]);
		}
	} else {
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (validity.RowIsValid(source_idx)) {
				TupleDataValueStore<T>(data[source_idx], target_locations[i], offset_in_row, target_heap_locations[i]);
			} else {
				TupleDataValueStore<T>(NullValue<T>(), target_locations[i], offset_in_row, target_heap_locations[i]);
				ValidityBytes(target_locations[i], layout.ColumnCount()).SetInvalidUnsafe(entry_idx, idx_in_entry);
			}
		}
	}
}

static void TupleDataStructScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                   const SelectionVector &append_sel, const idx_t append_count,
                                   const TupleDataLayout &layout, const Vector &row_locations, Vector &heap_locations,
                                   const idx_t col_idx, const UnifiedVectorFormat &dummy_arg,
                                   const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto &validity = source_data.validity;

	// Target
	const auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Set validity of the STRUCT in this layout
	if (!validity.AllValid()) {
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (!validity.RowIsValid(source_idx)) {
				ValidityBytes(target_locations[i], layout.ColumnCount()).SetInvalidUnsafe(entry_idx, idx_in_entry);
			}
		}
	}

	// Create a Vector of pointers to the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER, append_count);
	auto struct_target_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		struct_target_locations[i] = target_locations[i] + offset_in_row;
	}

	const auto &struct_layout = layout.GetStructLayout(col_idx);
	auto &struct_sources = StructVector::GetEntries(source);
	D_ASSERT(struct_layout.ColumnCount() == struct_sources.size());

	// Set the validity of the entries within the STRUCTs
	InitializeValidityMask(struct_target_locations, append_count,
	                       ValidityBytes::SizeInBytes(struct_layout.ColumnCount()));

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		auto &struct_source = *struct_sources[struct_col_idx];
		const auto &struct_source_format = source_format.children[struct_col_idx];
		const auto &struct_scatter_function = child_functions[struct_col_idx];
		struct_scatter_function.function(struct_source, struct_source_format, append_sel, append_count, struct_layout,
		                                 struct_row_locations, heap_locations, struct_col_idx, dummy_arg,
		                                 struct_scatter_function.child_functions);
	}
}

//------------------------------------------------------------------------------
// List Scatter
//------------------------------------------------------------------------------
static void TupleDataListScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                 const SelectionVector &append_sel, const idx_t append_count,
                                 const TupleDataLayout &layout, const Vector &row_locations, Vector &heap_locations,
                                 const idx_t col_idx, const UnifiedVectorFormat &,
                                 const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<list_entry_t>(source_data);
	const auto &validity = source_data.validity;

	// Target
	const auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Set validity of the LIST in this layout, and store pointer to where it's stored
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		const auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (validity.RowIsValid(source_idx)) {
			auto &target_heap_location = target_heap_locations[i];
			Store<data_ptr_t>(target_heap_location, target_locations[i] + offset_in_row);

			// Store list length and skip over it
			Store<uint64_t>(data[source_idx].length, target_heap_location);
			target_heap_location += sizeof(uint64_t);
		} else {
			ValidityBytes(target_locations[i], layout.ColumnCount()).SetInvalidUnsafe(entry_idx, idx_in_entry);
		}
	}

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	auto &child_source = ListVector::GetEntry(source);
	auto &child_format = source_format.children[0];
	const auto &child_function = child_functions[0];
	child_function.function(child_source, child_format, append_sel, append_count, layout, row_locations, heap_locations,
	                        col_idx, source_format.unified, child_function.child_functions);
}

//------------------------------------------------------------------------------
// Array Scatter
//------------------------------------------------------------------------------
static void TupleDataArrayScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                  const SelectionVector &append_sel, const idx_t append_count,
                                  const TupleDataLayout &layout, const Vector &row_locations, Vector &heap_locations,
                                  const idx_t col_idx, const UnifiedVectorFormat &,
                                  const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	// The Array vector has fake list_entry_t's set by this point, so this is fine
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<list_entry_t>(source_data);
	const auto &validity = source_data.validity;

	// Target
	const auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Set validity of the LIST in this layout, and store pointer to where it's stored
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		const auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (validity.RowIsValid(source_idx)) {
			auto &target_heap_location = target_heap_locations[i];
			Store<data_ptr_t>(target_heap_location, target_locations[i] + offset_in_row);

			// Store list length and skip over it
			Store<uint64_t>(data[source_idx].length, target_heap_location);
			target_heap_location += sizeof(uint64_t);
		} else {
			ValidityBytes(target_locations[i], layout.ColumnCount()).SetInvalidUnsafe(entry_idx, idx_in_entry);
		}
	}

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	auto &child_source = ArrayVector::GetEntry(source);
	auto &child_format = source_format.children[0];
	const auto &child_function = child_functions[0];
	child_function.function(child_source, child_format, append_sel, append_count, layout, row_locations, heap_locations,
	                        col_idx, source_format.unified, child_function.child_functions);
}

//------------------------------------------------------------------------------
// Collection Scatter
//------------------------------------------------------------------------------
template <class T>
static void TupleDataTemplatedWithinCollectionScatter(const Vector &, const TupleDataVectorFormat &source_format,
                                                      const SelectionVector &append_sel, const idx_t append_count,
                                                      const TupleDataLayout &, const Vector &, Vector &heap_locations,
                                                      const idx_t, const UnifiedVectorFormat &list_data,
                                                      const vector<TupleDataScatterFunction> &) {
	// Parent list data
	const auto &list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<T>(source_data);
	const auto &source_validity = source_data.validity;

	// Target
	const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;
		if (list_length == 0) {
			continue;
		}

		// Initialize validity mask and skip heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location, list_length);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto child_data_location = target_heap_location;
		target_heap_location += list_length * TupleDataWithinListFixedSize<T>();

		// Store the data and validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_source_idx = source_sel.get_index(list_offset + child_i);
			if (source_validity.RowIsValid(child_source_idx)) {
				TupleDataWithinListValueStore<T>(data[child_source_idx],
				                                 child_data_location + child_i * TupleDataWithinListFixedSize<T>(),
				                                 target_heap_location);
			} else {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}
	}
}

static void TupleDataStructWithinCollectionScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                                   const SelectionVector &append_sel, const idx_t append_count,
                                                   const TupleDataLayout &layout, const Vector &row_locations,
                                                   Vector &heap_locations, const idx_t,
                                                   const UnifiedVectorFormat &list_data,
                                                   const vector<TupleDataScatterFunction> &child_functions) {
	// Parent list data
	const auto &list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto &source_validity = source_data.validity;

	// Target
	const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Initialize the validity of the STRUCTs
	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;
		if (list_length == 0) {
			continue;
		}

		// Initialize validity mask and skip the heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location, list_length);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Store the validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_source_idx = source_sel.get_index(list_offset + child_i);
			if (!source_validity.RowIsValid(child_source_idx)) {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}
	}

	// Recurse through the children
	auto &struct_sources = StructVector::GetEntries(source);
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
		auto &struct_source = *struct_sources[struct_col_idx];
		auto &struct_format = source_format.children[struct_col_idx];
		const auto &struct_scatter_function = child_functions[struct_col_idx];
		struct_scatter_function.function(struct_source, struct_format, append_sel, append_count, layout, row_locations,
		                                 heap_locations, struct_col_idx, list_data,
		                                 struct_scatter_function.child_functions);
	}
}

template <class COLLECTION_VECTOR>
static void TupleDataCollectionWithinCollectionScatter(const Vector &child_list,
                                                       const TupleDataVectorFormat &child_list_format,
                                                       const SelectionVector &append_sel, const idx_t append_count,
                                                       const TupleDataLayout &layout, const Vector &row_locations,
                                                       Vector &heap_locations, const idx_t col_idx,
                                                       const UnifiedVectorFormat &list_data,
                                                       const vector<TupleDataScatterFunction> &child_functions) {
	// Parent list data
	const auto &list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Source
	const auto &child_list_data = child_list_format.unified;
	const auto &child_list_sel = *child_list_data.sel;
	const auto child_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(child_list_data);
	const auto &child_list_validity = child_list_data.validity;

	// Target
	const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child list
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;
		if (list_length == 0) {
			continue;
		}

		// Initialize validity mask and skip heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location, list_length);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto child_data_location = target_heap_location;
		target_heap_location += list_length * sizeof(uint64_t);

		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_list_idx = child_list_sel.get_index(list_offset + child_i);
			if (child_list_validity.RowIsValid(child_list_idx)) {
				const auto &child_list_length = child_list_entries[child_list_idx].length;
				Store<uint64_t>(child_list_length, child_data_location + child_i * sizeof(uint64_t));
			} else {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}
	}

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	auto &child_vec = COLLECTION_VECTOR::GetEntry(child_list);
	auto &child_format = child_list_format.children[0];
	auto &combined_child_list_data = child_format.combined_list_data->combined_data;
	const auto &child_function = child_functions[0];
	child_function.function(child_vec, child_format, append_sel, append_count, layout, row_locations, heap_locations,
	                        col_idx, combined_child_list_data, child_function.child_functions);
}

//------------------------------------------------------------------------------
// Get Scatter Function
//------------------------------------------------------------------------------
template <class T>
tuple_data_scatter_function_t TupleDataGetScatterFunction(bool within_collection) {
	return within_collection ? TupleDataTemplatedWithinCollectionScatter<T> : TupleDataTemplatedScatter<T>;
}

TupleDataScatterFunction TupleDataCollection::GetScatterFunction(const LogicalType &type, bool within_collection) {
	TupleDataScatterFunction result;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		result.function = TupleDataGetScatterFunction<bool>(within_collection);
		break;
	case PhysicalType::INT8:
		result.function = TupleDataGetScatterFunction<int8_t>(within_collection);
		break;
	case PhysicalType::INT16:
		result.function = TupleDataGetScatterFunction<int16_t>(within_collection);
		break;
	case PhysicalType::INT32:
		result.function = TupleDataGetScatterFunction<int32_t>(within_collection);
		break;
	case PhysicalType::INT64:
		result.function = TupleDataGetScatterFunction<int64_t>(within_collection);
		break;
	case PhysicalType::INT128:
		result.function = TupleDataGetScatterFunction<hugeint_t>(within_collection);
		break;
	case PhysicalType::UINT8:
		result.function = TupleDataGetScatterFunction<uint8_t>(within_collection);
		break;
	case PhysicalType::UINT16:
		result.function = TupleDataGetScatterFunction<uint16_t>(within_collection);
		break;
	case PhysicalType::UINT32:
		result.function = TupleDataGetScatterFunction<uint32_t>(within_collection);
		break;
	case PhysicalType::UINT64:
		result.function = TupleDataGetScatterFunction<uint64_t>(within_collection);
		break;
	case PhysicalType::UINT128:
		result.function = TupleDataGetScatterFunction<uhugeint_t>(within_collection);
		break;
	case PhysicalType::FLOAT:
		result.function = TupleDataGetScatterFunction<float>(within_collection);
		break;
	case PhysicalType::DOUBLE:
		result.function = TupleDataGetScatterFunction<double>(within_collection);
		break;
	case PhysicalType::INTERVAL:
		result.function = TupleDataGetScatterFunction<interval_t>(within_collection);
		break;
	case PhysicalType::VARCHAR:
		result.function = TupleDataGetScatterFunction<string_t>(within_collection);
		break;
	case PhysicalType::STRUCT: {
		result.function = within_collection ? TupleDataStructWithinCollectionScatter : TupleDataStructScatter;
		for (const auto &child_type : StructType::GetChildTypes(type)) {
			result.child_functions.push_back(GetScatterFunction(child_type.second, within_collection));
		}
		break;
	}
	case PhysicalType::LIST:
		result.function =
		    within_collection ? TupleDataCollectionWithinCollectionScatter<ListVector> : TupleDataListScatter;
		result.child_functions.emplace_back(GetScatterFunction(ListType::GetChildType(type), true));
		break;
	case PhysicalType::ARRAY:
		result.function =
		    within_collection ? TupleDataCollectionWithinCollectionScatter<ArrayVector> : TupleDataArrayScatter;
		result.child_functions.emplace_back(GetScatterFunction(ArrayType::GetChildType(type), true));
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetScatterFunction");
	}
	return result;
}

//-------------------------------------------------------------------------------
// Gather
//-------------------------------------------------------------------------------
void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 DataChunk &result, const SelectionVector &target_sel,
                                 vector<unique_ptr<Vector>> &cached_cast_vectors) const {
	D_ASSERT(result.ColumnCount() == layout.ColumnCount());
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}
	Gather(row_locations, scan_sel, scan_count, column_ids, result, target_sel, cached_cast_vectors);
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 const vector<column_t> &column_ids, DataChunk &result,
                                 const SelectionVector &target_sel,
                                 vector<unique_ptr<Vector>> &cached_cast_vectors) const {
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		Gather(row_locations, scan_sel, scan_count, column_ids[col_idx], result.data[col_idx], target_sel,
		       cached_cast_vectors[col_idx].get());
	}
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 const column_t column_id, Vector &result, const SelectionVector &target_sel,
                                 optional_ptr<Vector> cached_cast_vector) const {
	D_ASSERT(!cached_cast_vector || FlatVector::Validity(*cached_cast_vector).AllValid()); // ResetCachedCastVectors
	const auto &gather_function = gather_functions[column_id];
	gather_function.function(layout, row_locations, column_id, scan_sel, scan_count, result, target_sel,
	                         cached_cast_vector, gather_function.child_functions);
	Vector::Verify(result, target_sel, scan_count);
}

template <class T>
static void TupleDataTemplatedGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                     const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                     const SelectionVector &target_sel, optional_ptr<Vector>,
                                     const vector<TupleDataGatherFunction> &) {
	// Source
	const auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto target_data = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < scan_count; i++) {
		const auto &source_row = source_locations[scan_sel.get_index(i)];
		const auto target_idx = target_sel.get_index(i);
		target_data[target_idx] = Load<T>(source_row + offset_in_row);
		ValidityBytes row_mask(source_row, layout.ColumnCount());
		if (!row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
			target_validity.SetInvalid(target_idx);
		}
#ifdef DEBUG
		else {
			TupleDataValueVerify<T>(target.GetType(), target_data[target_idx]);
		}
#endif
	}
}

static void TupleDataStructGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                  const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                  const SelectionVector &target_sel, optional_ptr<Vector> dummy_vector,
                                  const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	const auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Get validity of the struct and create a Vector of pointers to the start of the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER);
	auto struct_source_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < scan_count; i++) {
		const auto source_idx = scan_sel.get_index(i);
		const auto &source_row = source_locations[source_idx];

		// Set the validity
		ValidityBytes row_mask(source_row, layout.ColumnCount());
		if (!row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
			const auto target_idx = target_sel.get_index(i);
			target_validity.SetInvalid(target_idx);
		}

		// Set the pointer
		struct_source_locations[source_idx] = source_row + offset_in_row;
	}

	// Get the struct layout and struct entries
	const auto &struct_layout = layout.GetStructLayout(col_idx);
	auto &struct_targets = StructVector::GetEntries(target);
	D_ASSERT(struct_layout.ColumnCount() == struct_targets.size());

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		auto &struct_target = *struct_targets[struct_col_idx];
		const auto &struct_gather_function = child_functions[struct_col_idx];
		struct_gather_function.function(struct_layout, struct_row_locations, struct_col_idx, scan_sel, scan_count,
		                                struct_target, target_sel, dummy_vector,
		                                struct_gather_function.child_functions);
	}
}

//------------------------------------------------------------------------------
// List Gather
//------------------------------------------------------------------------------
static void TupleDataListGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                const SelectionVector &target_sel, optional_ptr<Vector>,
                                const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	const auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	const auto target_list_entries = FlatVector::GetData<list_entry_t>(target);
	auto &target_list_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Load pointers to the data from the row
	Vector heap_locations(LogicalType::POINTER);
	const auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	uint64_t target_list_offset = 0;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto &source_row = source_locations[scan_sel.get_index(i)];
		ValidityBytes row_mask(source_row, layout.ColumnCount());

		const auto target_idx = target_sel.get_index(i);
		if (row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
			auto &source_heap_location = source_heap_locations[i];
			source_heap_location = Load<data_ptr_t>(source_row + offset_in_row);

			// Load list size and skip over
			const auto list_length = Load<uint64_t>(source_heap_location);
			source_heap_location += sizeof(uint64_t);

			// Initialize list entry, and increment offset
			auto &target_list_entry = target_list_entries[target_idx];
			target_list_entry.offset = target_list_offset;
			target_list_entry.length = list_length;
			target_list_offset += list_length;
		} else {
			target_list_validity.SetInvalid(target_idx);
		}
	}
	auto list_size_before = ListVector::GetListSize(target);
	ListVector::Reserve(target, list_size_before + target_list_offset);
	ListVector::SetListSize(target, list_size_before + target_list_offset);

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	const auto &child_function = child_functions[0];
	child_function.function(layout, heap_locations, list_size_before, scan_sel, scan_count,
	                        ListVector::GetEntry(target), target_sel, &target, child_function.child_functions);
}

//------------------------------------------------------------------------------
// Collection Gather
//------------------------------------------------------------------------------
template <class T>
static void
TupleDataTemplatedWithinCollectionGather(const TupleDataLayout &, Vector &heap_locations, const idx_t list_size_before,
                                         const SelectionVector &, const idx_t scan_count, Vector &target,
                                         const SelectionVector &target_sel, optional_ptr<Vector> list_vector,
                                         const vector<TupleDataGatherFunction> &) {
	// List parent
	const auto list_entries = FlatVector::GetData<list_entry_t>(*list_vector);
	const auto &list_validity = FlatVector::Validity(*list_vector);

	// Source
	const auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Target
	const auto target_data = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	uint64_t target_offset = list_size_before;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto target_idx = target_sel.get_index(i);
		if (!list_validity.RowIsValid(target_idx)) {
			continue;
		}

		const auto &list_length = list_entries[target_idx].length;
		if (list_length == 0) {
			continue;
		}

		// Initialize validity mask
		auto &source_heap_location = source_heap_locations[i];
		ValidityBytes source_mask(source_heap_location, list_length);
		source_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto source_data_location = source_heap_location;
		source_heap_location += list_length * TupleDataWithinListFixedSize<T>();

		// Load the child validity and data belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			if (source_mask.RowIsValidUnsafe(child_i)) {
				auto &target_value = target_data[target_offset + child_i];
				target_value = TupleDataWithinListValueLoad<T>(
				    source_data_location + child_i * TupleDataWithinListFixedSize<T>(), source_heap_location);
				TupleDataValueVerify(target.GetType(), target_value);
			} else {
				target_validity.SetInvalid(target_offset + child_i);
			}
		}
		target_offset += list_length;
	}
}

static void TupleDataStructWithinCollectionGather(const TupleDataLayout &layout, Vector &heap_locations,
                                                  const idx_t list_size_before, const SelectionVector &scan_sel,
                                                  const idx_t scan_count, Vector &target,
                                                  const SelectionVector &target_sel, optional_ptr<Vector> list_vector,
                                                  const vector<TupleDataGatherFunction> &child_functions) {
	// List parent
	const auto list_entries = FlatVector::GetData<list_entry_t>(*list_vector);
	const auto &list_validity = FlatVector::Validity(*list_vector);

	// Source
	const auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Target
	auto &target_validity = FlatVector::Validity(target);

	uint64_t target_offset = list_size_before;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto target_idx = target_sel.get_index(i);
		if (!list_validity.RowIsValid(target_idx)) {
			continue;
		}

		const auto &list_length = list_entries[target_idx].length;
		if (list_length == 0) {
			continue;
		}

		// Initialize validity mask and skip over it
		auto &source_heap_location = source_heap_locations[i];
		ValidityBytes source_mask(source_heap_location, list_length);
		source_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Load the child validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			if (!source_mask.RowIsValidUnsafe(child_i)) {
				target_validity.SetInvalid(target_offset + child_i);
			}
		}
		target_offset += list_length;
	}

	// Recurse
	auto &struct_targets = StructVector::GetEntries(target);
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_targets.size(); struct_col_idx++) {
		auto &struct_target = *struct_targets[struct_col_idx];
		const auto &struct_gather_function = child_functions[struct_col_idx];
		struct_gather_function.function(layout, heap_locations, list_size_before, scan_sel, scan_count, struct_target,
		                                target_sel, list_vector, struct_gather_function.child_functions);
	}
}

static void TupleDataCollectionWithinCollectionGather(const TupleDataLayout &layout, Vector &heap_locations,
                                                      const idx_t list_size_before, const SelectionVector &scan_sel,
                                                      const idx_t scan_count, Vector &target,
                                                      const SelectionVector &target_sel,
                                                      optional_ptr<Vector> list_vector,
                                                      const vector<TupleDataGatherFunction> &child_functions) {
	// List parent
	const auto list_entries = FlatVector::GetData<list_entry_t>(*list_vector);
	const auto &list_validity = FlatVector::Validity(*list_vector);

	// Source
	const auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Target
	const auto target_list_entries = FlatVector::GetData<list_entry_t>(target);
	auto &target_validity = FlatVector::Validity(target);
	const auto child_list_size_before = ListVector::GetListSize(target);

	// We need to create a vector that has the combined list sizes (hugeint_t has same size as list_entry_t)
	Vector combined_list_vector(LogicalType::HUGEINT);
	FlatVector::SetValidity(combined_list_vector, list_validity); // Has same validity as list parent
	const auto combined_list_entries = FlatVector::GetData<list_entry_t>(combined_list_vector);

	uint64_t target_offset = list_size_before;
	uint64_t target_child_offset = child_list_size_before;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto target_idx = target_sel.get_index(i);
		if (!list_validity.RowIsValid(target_idx)) {
			continue;
		}

		// Set the offset of the combined list entry
		auto &combined_list_entry = combined_list_entries[target_idx];
		combined_list_entry.offset = target_child_offset;

		const auto &list_length = list_entries[target_idx].length;
		if (list_length == 0) {
			combined_list_entry.length = 0;
			continue;
		}

		// Initialize validity mask and skip over it
		auto &source_heap_location = source_heap_locations[i];
		ValidityBytes source_mask(source_heap_location, list_length);
		source_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto source_data_location = source_heap_location;
		source_heap_location += list_length * sizeof(uint64_t);

		// Load the child validity and data belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			if (source_mask.RowIsValidUnsafe(child_i)) {
				auto &target_list_entry = target_list_entries[target_offset + child_i];
				target_list_entry.offset = target_child_offset;
				target_list_entry.length = Load<uint64_t>(source_data_location + child_i * sizeof(uint64_t));
				target_child_offset += target_list_entry.length;
			} else {
				target_validity.SetInvalid(target_offset + child_i);
			}
		}

		// Set the length of the combined list entry
		combined_list_entry.length = target_child_offset - combined_list_entry.offset;

		target_offset += list_length;
	}

	ListVector::Reserve(target, target_child_offset);
	ListVector::SetListSize(target, target_child_offset);

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	const auto &child_function = child_functions[0];
	child_function.function(layout, heap_locations, child_list_size_before, scan_sel, scan_count,
	                        ListVector::GetEntry(target), target_sel, &combined_list_vector,
	                        child_function.child_functions);
}

//------------------------------------------------------------------------------
// Special cases for arrays
//------------------------------------------------------------------------------
// A gather function that wraps another gather function and casts the result to the target array type
static void TupleDataCastToArrayListGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                           const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                           const SelectionVector &target_sel, optional_ptr<Vector> cached_cast_vector,
                                           const vector<TupleDataGatherFunction> &child_functions) {
	if (cached_cast_vector) {
		// Reuse the cached cast vector
		TupleDataListGather(layout, row_locations, col_idx, scan_sel, scan_count, *cached_cast_vector, target_sel,
		                    cached_cast_vector, child_functions);
		VectorOperations::DefaultCast(*cached_cast_vector, target, scan_count);
	} else {
		// Otherwise, create a new temporary cast vector
		Vector cast_vector(ArrayType::ConvertToList(target.GetType()));
		TupleDataListGather(layout, row_locations, col_idx, scan_sel, scan_count, cast_vector, target_sel, &cast_vector,
		                    child_functions);
		VectorOperations::DefaultCast(cast_vector, target, scan_count);
	}
}

static void TupleDataCastToArrayStructGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                             const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                             const SelectionVector &target_sel, optional_ptr<Vector> cached_cast_vector,
                                             const vector<TupleDataGatherFunction> &child_functions) {

	if (cached_cast_vector) {
		// Reuse the cached cast vector
		TupleDataStructGather(layout, row_locations, col_idx, scan_sel, scan_count, *cached_cast_vector, target_sel,
		                      cached_cast_vector, child_functions);
		VectorOperations::DefaultCast(*cached_cast_vector, target, scan_count);
	} else {
		// Otherwise, create a new temporary cast vector
		Vector cast_vector(ArrayType::ConvertToList(target.GetType()));
		TupleDataStructGather(layout, row_locations, col_idx, scan_sel, scan_count, cast_vector, target_sel,
		                      &cast_vector, child_functions);
		VectorOperations::DefaultCast(cast_vector, target, scan_count);
	}
}

//------------------------------------------------------------------------------
// Get Gather Function
//------------------------------------------------------------------------------
template <class T>
tuple_data_gather_function_t TupleDataGetGatherFunction(bool within_collection) {
	return within_collection ? TupleDataTemplatedWithinCollectionGather<T> : TupleDataTemplatedGather<T>;
}

static TupleDataGatherFunction TupleDataGetGatherFunctionInternal(const LogicalType &type, bool within_collection) {
	TupleDataGatherFunction result;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		result.function = TupleDataGetGatherFunction<bool>(within_collection);
		break;
	case PhysicalType::INT8:
		result.function = TupleDataGetGatherFunction<int8_t>(within_collection);
		break;
	case PhysicalType::INT16:
		result.function = TupleDataGetGatherFunction<int16_t>(within_collection);
		break;
	case PhysicalType::INT32:
		result.function = TupleDataGetGatherFunction<int32_t>(within_collection);
		break;
	case PhysicalType::INT64:
		result.function = TupleDataGetGatherFunction<int64_t>(within_collection);
		break;
	case PhysicalType::INT128:
		result.function = TupleDataGetGatherFunction<hugeint_t>(within_collection);
		break;
	case PhysicalType::UINT8:
		result.function = TupleDataGetGatherFunction<uint8_t>(within_collection);
		break;
	case PhysicalType::UINT16:
		result.function = TupleDataGetGatherFunction<uint16_t>(within_collection);
		break;
	case PhysicalType::UINT32:
		result.function = TupleDataGetGatherFunction<uint32_t>(within_collection);
		break;
	case PhysicalType::UINT64:
		result.function = TupleDataGetGatherFunction<uint64_t>(within_collection);
		break;
	case PhysicalType::UINT128:
		result.function = TupleDataGetGatherFunction<uhugeint_t>(within_collection);
		break;
	case PhysicalType::FLOAT:
		result.function = TupleDataGetGatherFunction<float>(within_collection);
		break;
	case PhysicalType::DOUBLE:
		result.function = TupleDataGetGatherFunction<double>(within_collection);
		break;
	case PhysicalType::INTERVAL:
		result.function = TupleDataGetGatherFunction<interval_t>(within_collection);
		break;
	case PhysicalType::VARCHAR:
		result.function = TupleDataGetGatherFunction<string_t>(within_collection);
		break;
	case PhysicalType::STRUCT: {
		result.function = within_collection ? TupleDataStructWithinCollectionGather : TupleDataStructGather;
		for (const auto &child_type : StructType::GetChildTypes(type)) {
			result.child_functions.push_back(TupleDataGetGatherFunctionInternal(child_type.second, within_collection));
		}
		break;
	}
	case PhysicalType::LIST:
		result.function = within_collection ? TupleDataCollectionWithinCollectionGather : TupleDataListGather;
		result.child_functions.push_back(TupleDataGetGatherFunctionInternal(ListType::GetChildType(type), true));
		break;
	case PhysicalType::ARRAY:
		result.function = within_collection ? TupleDataCollectionWithinCollectionGather : TupleDataListGather;
		result.child_functions.push_back(TupleDataGetGatherFunctionInternal(ArrayType::GetChildType(type), true));
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetGatherFunction");
	}
	return result;
}

TupleDataGatherFunction TupleDataCollection::GetGatherFunction(const LogicalType &type) {
	if (!type.IsNested()) {
		return TupleDataGetGatherFunctionInternal(type, false);
	}

	if (TypeVisitor::Contains(type, LogicalTypeId::ARRAY)) {
		// Special case: we cant handle arrays yet, so we need to replace them with lists when gathering
		const auto new_type = ArrayType::ConvertToList(type);
		TupleDataGatherFunction result;
		// Theres only two cases: Either the array is within a struct, or it is within a list (or has now become a list)
		switch (new_type.InternalType()) {
		case PhysicalType::LIST:
			result.function = TupleDataCastToArrayListGather;
			result.child_functions.push_back(
			    TupleDataGetGatherFunctionInternal(ListType::GetChildType(new_type), true));
			return result;
		case PhysicalType::STRUCT:
			result.function = TupleDataCastToArrayStructGather;
			for (const auto &child_type : StructType::GetChildTypes(new_type)) {
				result.child_functions.push_back(TupleDataGetGatherFunctionInternal(child_type.second, false));
			}
			return result;
		default:
			throw InternalException("Unsupported type for TupleDataCollection::GetGatherFunction");
		}
	}
	return TupleDataGetGatherFunctionInternal(type, false);
}

} // namespace duckdb
