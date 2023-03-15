#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_allocator.hpp"

#include <algorithm>

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

typedef void (*tuple_data_scatter_function_t)(Vector &source, const UnifiedVectorFormat &source_data,
                                              const SelectionVector &append_sel, const idx_t append_count,
                                              const idx_t original_count, const TupleDataLayout &layout,
                                              Vector &row_locations, Vector &heap_locations, const idx_t col_idx,
                                              const vector<TupleDataScatterFunction> &child_functions);

struct TupleDataScatterFunction {
	tuple_data_scatter_function_t function;
	vector<TupleDataScatterFunction> child_functions;
};

typedef void (*tuple_data_gather_function_t)(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                             const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                             const SelectionVector &target_sel,
                                             const vector<TupleDataGatherFunction> &child_functions);

struct TupleDataGatherFunction {
	tuple_data_gather_function_t function;
	vector<TupleDataGatherFunction> child_functions;
};

TupleDataCollection::TupleDataCollection(BufferManager &buffer_manager, const TupleDataLayout &layout_p)
    : layout(layout_p), allocator(make_shared<TupleDataAllocator>(buffer_manager, layout)) {
	Initialize();
}

TupleDataCollection::TupleDataCollection(shared_ptr<TupleDataAllocator> allocator)
    : layout(allocator->GetLayout()), allocator(std::move(allocator)) {
	Initialize();
}

TupleDataCollection::~TupleDataCollection() {
}

void TupleDataCollection::Initialize() {
	D_ASSERT(!layout.GetTypes().empty());
	this->count = 0;
	scatter_functions.reserve(layout.ColumnCount());
	gather_functions.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		auto &type = layout.GetTypes()[col_idx];
		scatter_functions.emplace_back(GetScatterFunction(type, layout, col_idx));
		gather_functions.emplace_back(GetGatherFunction(type, layout, col_idx));
	}
}

const TupleDataLayout &TupleDataCollection::GetLayout() const {
	return layout;
}

const idx_t &TupleDataCollection::Count() const {
	return count;
}

idx_t TupleDataCollection::ChunkCount() const {
	idx_t total_chunk_count = 0;
	for (const auto &segment : segments) {
		total_chunk_count += segment.ChunkCount();
	}
	return total_chunk_count;
}

idx_t TupleDataCollection::SizeInBytes() const {
	idx_t total_size = 0;
	for (const auto &segment : segments) {
		total_size += segment.SizeInBytes();
	}
	return total_size;
}

void VerifyAppendColumns(const TupleDataLayout &layout, const vector<column_t> &column_ids) {
#ifdef DEBUG
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		if (std::find(column_ids.begin(), column_ids.end(), col_idx) != column_ids.end()) {
			continue;
		}
		// This column will not be appended in the first go - verify that it is fixed-size - we cannot resize heap after
		const auto physical_type = layout.GetTypes()[col_idx].InternalType();
		D_ASSERT(physical_type != PhysicalType::VARCHAR && physical_type != PhysicalType::LIST);
		if (physical_type == PhysicalType::STRUCT) {
			const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
			vector<column_t> struct_column_ids;
			struct_column_ids.reserve(struct_layout.ColumnCount());
			for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
				struct_column_ids.emplace_back(struct_col_idx);
			}
			VerifyAppendColumns(struct_layout, struct_column_ids);
		}
	}
#endif
}

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, TupleDataPinProperties properties) {
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}
	InitializeAppend(append_state, std::move(column_ids), properties);
}

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids,
                                           TupleDataPinProperties properties) {
	VerifyAppendColumns(layout, column_ids);
	InitializeAppend(append_state.pin_state, properties);
	InitializeAppend(append_state.chunk_state, std::move(column_ids));
}

void TupleDataCollection::InitializeAppend(TupleDataPinState &pin_state, TupleDataPinProperties properties) {
	pin_state.properties = properties;
	if (segments.empty()) {
		segments.emplace_back(allocator);
	}
}

void TupleDataCollection::InitializeAppend(TupleDataChunkState &chunk_state, vector<column_t> column_ids) {
	chunk_state.vector_data.resize(layout.ColumnCount());
	chunk_state.column_ids = std::move(column_ids);
}

void TupleDataCollection::Append(DataChunk &new_chunk, const SelectionVector &append_sel, idx_t append_count) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state);
	Append(append_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(DataChunk &new_chunk, vector<column_t> column_ids, const SelectionVector &append_sel,
                                 idx_t append_count) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state, std::move(column_ids));
	Append(append_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(TupleDataAppendState &append_state, DataChunk &new_chunk,
                                 const SelectionVector &append_sel, idx_t append_count) {
	Append(append_state.pin_state, append_state.chunk_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, DataChunk &new_chunk,
                                 const SelectionVector &append_sel, idx_t append_count) {
	D_ASSERT(chunk_state.vector_data.size() == layout.ColumnCount()); // Needs InitializeAppend
	if (append_count == DConstants::INVALID_INDEX) {
		append_count = new_chunk.size();
	}

	TupleDataCollection::ToUnifiedFormat(chunk_state, new_chunk);

	if (!layout.AllConstant()) {
		TupleDataCollection::ComputeHeapSizes(chunk_state, new_chunk, append_sel, append_count);
	}

	Build(pin_state, chunk_state, 0, append_count);

	Scatter(chunk_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::ToUnifiedFormat(TupleDataChunkState &chunk_state, DataChunk &new_chunk) {
	for (const auto &col_idx : chunk_state.column_ids) {
		new_chunk.data[col_idx].ToUnifiedFormat(new_chunk.size(), chunk_state.vector_data[col_idx]);
	}
}

void TupleDataCollection::ComputeHeapSizes(Vector &heap_sizes_v, Vector &source_v, UnifiedVectorFormat &source,
                                           const SelectionVector &append_sel, const idx_t append_count,
                                           const idx_t original_count) {
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	switch (source_v.GetType().InternalType()) {
	case PhysicalType::VARCHAR: {
		// Only non-inlined strings are stored in the heap
		const auto source_data = (string_t *)source.data;
		const auto &source_sel = *source.sel;
		const auto &source_validity = source.validity;
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (source_validity.RowIsValid(source_idx) && !source_data[source_idx].IsInlined()) {
				heap_sizes[i] += source_data[source_idx].GetSize();
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		// Recurse through the struct children
		auto &struct_sources = StructVector::GetEntries(source_v);
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
			const auto &struct_source = struct_sources[struct_col_idx];
			UnifiedVectorFormat struct_source_data;
			struct_source->ToUnifiedFormat(append_count, struct_source_data);
			ComputeHeapSizes(heap_sizes_v, *struct_source, struct_source_data, append_sel, append_count,
			                 original_count);
		}
		break;
	}
	case PhysicalType::LIST: {
		// Use the old code until we re-write the list serialization
		RowOperations::ComputeEntrySizes(source_v, source, heap_sizes, original_count, append_count, append_sel);
		break;
	}
	default:
		return;
	}
}

void TupleDataCollection::Build(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                const idx_t append_offset, const idx_t append_count) {
	segments.back().allocator->Build(segments.back(), pin_state, chunk_state, append_offset, append_count);
	count += append_count;
	Verify();
}

void TupleDataCollection::Scatter(TupleDataChunkState &chunk_state, DataChunk &new_chunk,
                                  const SelectionVector &append_sel, const idx_t append_count) const {
	const auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);

	// Set the validity mask for each row before inserting data
	const auto validity_bytes = ValidityBytes::SizeInBytes(layout.ColumnCount());
	for (idx_t i = 0; i < append_count; i++) {
		memset(row_locations[i], ~0, validity_bytes);
	}

	if (!layout.AllConstant()) {
		// Set the heap size for each row
		const auto heap_size_offset = layout.GetHeapSizeOffset();
		const auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
		for (idx_t i = 0; i < append_count; i++) {
			Store<uint32_t>(heap_sizes[i], row_locations[i] + heap_size_offset);
		}
	}

	// Write the data
	for (const auto &col_idx : chunk_state.column_ids) {
		Scatter(chunk_state, new_chunk.data[col_idx], col_idx, append_sel, append_count, new_chunk.size());
	}
}

void TupleDataCollection::Scatter(TupleDataChunkState &chunk_state, Vector &source, const column_t column_id,
                                  const SelectionVector &append_sel, const idx_t append_count,
                                  const idx_t original_count) const {
	const auto &scatter_function = scatter_functions[column_id];
	scatter_function.function(source, chunk_state.vector_data[column_id], append_sel, append_count, original_count,
	                          layout, chunk_state.row_locations, chunk_state.heap_locations, column_id,
	                          scatter_function.child_functions);
}

void VerifyHeapSizes(const data_ptr_t source_locations[], const idx_t heap_sizes[], const SelectionVector &append_sel,
                     const idx_t append_count, const idx_t heap_size_offset) {
#ifdef DEBUG
	for (idx_t i = 0; i < append_count; i++) {
		auto idx = append_sel.get_index(i);
		const auto stored_heap_size = Load<uint32_t>(source_locations[idx] + heap_size_offset);
		D_ASSERT(stored_heap_size == heap_sizes[idx]);
	}
#endif
}

void TupleDataCollection::CopyRows(TupleDataChunkState &chunk_state, TupleDataChunkState &input,
                                   const SelectionVector &append_sel, const idx_t append_count) const {
	const auto source_locations = FlatVector::GetData<data_ptr_t>(input.row_locations);
	const auto target_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);

	// Copy rows
	const auto row_width = layout.GetRowWidth();
	for (idx_t i = 0; i < append_count; i++) {
		auto idx = append_sel.get_index(i);
		FastMemcpy(target_locations[i], source_locations[idx], row_width);
	}

	// Copy heap if we need to
	if (!layout.AllConstant()) {
		const auto source_heap_locations = FlatVector::GetData<data_ptr_t>(input.heap_locations);
		const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);
		const auto heap_sizes = FlatVector::GetData<idx_t>(input.heap_sizes);
		VerifyHeapSizes(source_locations, heap_sizes, append_sel, append_count, layout.GetHeapSizeOffset());

		// Check if we need to copy anything at all
		idx_t total_heap_size = 0;
		for (idx_t i = 0; i < append_count; i++) {
			auto idx = append_sel.get_index(i);
			total_heap_size += heap_sizes[idx];
		}
		if (total_heap_size == 0) {
			return;
		}

		// Copy heap
		for (idx_t i = 0; i < append_count; i++) {
			auto idx = append_sel.get_index(i);
			FastMemcpy(target_heap_locations[i], source_heap_locations[idx], heap_sizes[idx]);
		}

		// Recompute pointers after copying the data
		TupleDataAllocator::RecomputeHeapPointers(input.heap_locations, append_sel, target_locations,
		                                          chunk_state.heap_locations, 0, append_count, layout, 0);
	}
}

void TupleDataCollection::Combine(TupleDataCollection &other) {
	if (other.count == 0) {
		return;
	}
	if (this->layout.GetTypes() != other.GetLayout().GetTypes()) {
		throw InternalException("Attempting to combine TupleDataCollection with mismatching types");
	}
	if (this->segments.size() == 1 && other.segments.size() == 1 &&
	    this->segments[0].allocator.get() == other.segments[0].allocator.get()) {
		// Same allocator - combine segments
		this->segments[0].Combine(other.segments[0]);
	} else {
		// Different allocator - append segments
		for (auto &other_seg : other.segments) {
			this->segments.emplace_back(std::move(other_seg));
		}
	}
	this->count += other.count;
	other.Reset();
	Verify();
}

void TupleDataCollection::Reset() {
	count = 0;
	segments.clear();

	// Refreshes the TupleDataAllocator to prevent holding on to allocated data unnecessarily
	allocator = make_shared<TupleDataAllocator>(*allocator);
}

void TupleDataCollection::InitializeChunk(DataChunk &chunk) const {
	chunk.Initialize(allocator->GetAllocator(), layout.GetTypes());
}

void TupleDataCollection::InitializeScanChunk(TupleDataScanState &state, DataChunk &chunk) const {
	auto &column_ids = state.chunk_state.column_ids;
	D_ASSERT(!column_ids.empty());
	vector<LogicalType> chunk_types;
	chunk_types.reserve(column_ids.size());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_idx = column_ids[i];
		D_ASSERT(column_idx < layout.ColumnCount());
		chunk_types.push_back(layout.GetTypes()[column_idx]);
	}
	chunk.Initialize(allocator->GetAllocator(), chunk_types);
}

void TupleDataCollection::InitializeScan(TupleDataScanState &state, TupleDataScanProperties properties) const {
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		column_ids.push_back(i);
	}
	InitializeScan(state, std::move(column_ids), properties);
}

void TupleDataCollection::InitializeScan(TupleDataScanState &state, vector<column_t> column_ids,
                                         TupleDataScanProperties properties) const {
	state.pin_state.row_handles.clear();
	state.pin_state.heap_handles.clear();
	state.pin_state.properties = TupleDataPinProperties::UNPIN_AFTER_DONE;
	state.segment_index = 0;
	state.chunk_index = 0;
	state.properties = properties;
	state.chunk_state.column_ids = std::move(column_ids);
}

void TupleDataCollection::InitializeScan(TupleDataParallelScanState &gstate, TupleDataScanProperties properties) const {
	InitializeScan(gstate.scan_state, properties);
}

void TupleDataCollection::InitializeScan(TupleDataParallelScanState &state, vector<column_t> column_ids,
                                         TupleDataScanProperties properties) const {
	InitializeScan(state.scan_state, std::move(column_ids), properties);
}

bool TupleDataCollection::Scan(TupleDataScanState &state, DataChunk &result) {
	const auto segment_index_before = state.segment_index;
	idx_t segment_index;
	idx_t chunk_index;
	if (!NextScanIndex(state, segment_index, chunk_index)) {
		return false;
	}
	if (segment_index != segment_index_before) {
		state.pin_state.row_handles.clear();
		state.pin_state.heap_handles.clear();
	}
	ScanAtIndex(state.pin_state, state.chunk_state, state.chunk_state.column_ids, segment_index, chunk_index, result);
	return true;
}

bool TupleDataCollection::Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result) {
	auto &scan_state = lstate.scan_state;
	const auto segment_index_before = scan_state.segment_index;
	{
		lock_guard<mutex> guard(gstate.lock);
		if (!NextScanIndex(gstate.scan_state, scan_state.segment_index, scan_state.chunk_index)) {
			return false;
		}
	}
	if (scan_state.segment_index != segment_index_before) {
		scan_state.pin_state.row_handles.clear();
		scan_state.pin_state.heap_handles.clear();
	}
	ScanAtIndex(scan_state.pin_state, scan_state.chunk_state, gstate.scan_state.chunk_state.column_ids,
	            scan_state.segment_index, scan_state.chunk_index, result);
	return true;
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 DataChunk &result, const SelectionVector &target_sel) const {
	D_ASSERT(result.ColumnCount() == layout.ColumnCount());
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}
	Gather(row_locations, scan_sel, scan_count, column_ids, result, target_sel);
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 const vector<column_t> &column_ids, DataChunk &result,
                                 const SelectionVector &target_sel) const {
	D_ASSERT(column_ids.size() == result.ColumnCount());
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		Gather(row_locations, scan_sel, scan_count, column_ids[col_idx], result.data[col_idx], target_sel);
	}
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 const column_t column_id, Vector &result, const SelectionVector &target_sel) const {
	const auto &gather_function = gather_functions[column_id];
	gather_function.function(layout, row_locations, column_id, scan_sel, scan_count, result, target_sel,
	                         gather_function.child_functions);
}

void TupleDataCollection::ComputeHeapSizes(TupleDataChunkState &chunk_state, DataChunk &new_chunk,
                                           const SelectionVector &append_sel, const idx_t append_count) {
	auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
	std::fill_n(heap_sizes, new_chunk.size(), 0);

	for (idx_t col_idx = 0; col_idx < new_chunk.ColumnCount(); col_idx++) {
		TupleDataCollection::ComputeHeapSizes(chunk_state.heap_sizes, new_chunk.data[col_idx],
		                                      chunk_state.vector_data[col_idx], append_sel, append_count,
		                                      new_chunk.size());
	}
}

template <class T>
static constexpr idx_t TupleDataWithinListFixedSize() {
	return sizeof(T);
}

template <>
constexpr idx_t TupleDataWithinListFixedSize<string_t>() {
	return sizeof(uint32_t);
}

template <>
constexpr idx_t TupleDataWithinListFixedSize<list_entry_t>() {
	return sizeof(uint64_t);
}

template <class T>
static inline void TupleDataValueStore(const T &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                       data_ptr_t &heap_location) {
	Store<T>(source, row_location + offset_in_row);
}

template <>
inline void TupleDataValueStore(const string_t &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                data_ptr_t &heap_location) {
	if (source.IsInlined()) {
		Store<string_t>(source, row_location + offset_in_row);
	} else {
		memcpy(heap_location, source.GetDataUnsafe(), source.GetSize());
		Store<string_t>(string_t((const char *)heap_location, source.GetSize()), row_location + offset_in_row);
		heap_location += source.GetSize();
	}
}

template <class T>
static inline void TupleDataWithinListValueStore(const T &source, const data_ptr_t &location,
                                                 data_ptr_t &heap_location) {
	Store<T>(source, location);
}

template <>
inline void TupleDataWithinListValueStore(const string_t &source, const data_ptr_t &location,
                                          data_ptr_t &heap_location) {
	Store<uint32_t>(source.GetSize(), location);
	memcpy(heap_location, source.GetDataUnsafe(), source.GetSize());
	heap_location += source.GetSize();
}

template <>
inline void TupleDataWithinListValueStore(const list_entry_t &source, const data_ptr_t &location,
                                          data_ptr_t &heap_location) {
	Store<uint64_t>(source.length, location);
}

template <class T>
static inline T TupleDataWithinListValueLoad(const data_ptr_t &location, data_ptr_t &heap_location) {
	return Load<T>(location);
}

template <>
inline string_t TupleDataWithinListValueLoad(const data_ptr_t &location, data_ptr_t &heap_location) {
	const auto size = Load<uint32_t>(location);
	string_t result((const char *)heap_location, size);
	heap_location += size;
	return result;
}

template <class T>
static void TemplatedTupleDataScatter(Vector &source, const UnifiedVectorFormat &source_data,
                                      const SelectionVector &append_sel, const idx_t append_count,
                                      const idx_t original_count, const TupleDataLayout &layout, Vector &row_locations,
                                      Vector &heap_locations, const idx_t col_idx,
                                      const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto source_sel = *source_data.sel;
	const auto data = (T *)source_data.data;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	if (validity.AllValid()) {
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_sel.get_index(append_sel.get_index(i));
			TupleDataValueStore<T>(data[source_idx], target_locations[i], offset_in_row, target_heap_locations[i]);
		}
	} else {
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (validity.RowIsValid(source_idx)) {
				TupleDataValueStore<T>(data[source_idx], target_locations[i], offset_in_row, target_heap_locations[i]);
			} else {
				TupleDataValueStore<T>(NullValue<T>(), target_locations[i], offset_in_row, target_heap_locations[i]);
				ValidityBytes(target_locations[i]).SetInvalidUnsafe(entry_idx, idx_in_entry);
			}
		}
	}
}

static void StructTupleDataScatter(Vector &source, const UnifiedVectorFormat &source_data,
                                   const SelectionVector &append_sel, const idx_t append_count,
                                   const idx_t original_count, const TupleDataLayout &layout, Vector &row_locations,
                                   Vector &heap_locations, const idx_t col_idx,
                                   const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto source_sel = *source_data.sel;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Set validity of the STRUCT in this layout
	for (idx_t i = 0; i < append_count; i++) {
		auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (!validity.RowIsValid(source_idx)) {
			ValidityBytes(target_locations[i]).SetInvalidUnsafe(entry_idx, idx_in_entry);
		}
	}

	// Create a Vector of pointers to the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER, append_count);
	auto struct_target_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		struct_target_locations[i] = target_locations[i] + offset_in_row;
	}

	D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
	const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
	auto &struct_sources = StructVector::GetEntries(source);
	D_ASSERT(struct_layout.ColumnCount() == struct_sources.size());

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		const auto &struct_source = struct_sources[struct_col_idx];
		UnifiedVectorFormat struct_source_data;
		struct_source->ToUnifiedFormat(original_count, struct_source_data);

		const auto &struct_scatter_function = child_functions[col_idx];
		struct_scatter_function.function(*struct_sources[struct_col_idx], struct_source_data, append_sel, append_count,
		                                 original_count, struct_layout, struct_row_locations, heap_locations,
		                                 struct_col_idx, struct_scatter_function.child_functions);
	}
}

static void ListTupleDataScatter(Vector &source, const UnifiedVectorFormat &source_data,
                                 const SelectionVector &append_sel, const idx_t append_count,
                                 const idx_t original_count, const TupleDataLayout &layout, Vector &row_locations,
                                 Vector &heap_locations, const idx_t col_idx,
                                 const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto source_sel = *source_data.sel;
	const auto data = (list_entry_t *)source_data.data;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Set validity of the LIST in this layout, and store pointer to where it's stored
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (validity.RowIsValid(source_idx)) {
			auto &target_heap_location = target_heap_locations[i];
			Store<data_ptr_t>(target_heap_location, target_locations[i] + offset_in_row);

			// Store list length and skip over it
			TupleDataWithinListValueStore<list_entry_t>(data[source_idx], target_heap_location, target_heap_location);
			target_heap_location += TupleDataWithinListFixedSize<list_entry_t>();
		} else {
			ValidityBytes(target_locations[i]).SetInvalidUnsafe(entry_idx, idx_in_entry);
		}
	}

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	const auto &child_function = child_functions[0];
	child_function.function(ListVector::GetEntry(source), source_data, append_sel, append_count,
	                        ListVector::GetListSize(source), layout, row_locations, heap_locations, col_idx,
	                        child_function.child_functions);
}

template <class T>
static void TemplatedWithinListTupleDataScatter(Vector &source, const UnifiedVectorFormat &list_data,
                                                const SelectionVector &append_sel, const idx_t append_count,
                                                const idx_t source_count, const TupleDataLayout &layout,
                                                Vector &row_locations, Vector &heap_locations, const idx_t col_idx,
                                                const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	UnifiedVectorFormat source_data;
	source.ToUnifiedFormat(source_count, source_data);
	const auto source_sel = *source_data.sel;
	const auto data = (T *)source_data.data;
	const auto &source_validity = source_data.validity;

	// List data
	const auto list_sel = *list_data.sel;
	const auto list_entries = (list_entry_t *)list_data.data;
	const auto &list_validity = list_data.validity;

	// Target
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	for (idx_t i = 0; i < append_count; i++) {
		auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(source_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[source_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Initialize validity mask and skip heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto child_data_location = target_heap_location;
		target_heap_location += list_length * TupleDataWithinListFixedSize<T>();

		// Store the data and validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			auto child_source_idx = source_sel.get_index(list_offset + child_i);
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

static void StructWithinListTupleDataScatter(Vector &source, const UnifiedVectorFormat &list_data,
                                             const SelectionVector &append_sel, const idx_t append_count,
                                             const idx_t source_count, const TupleDataLayout &layout,
                                             Vector &row_locations, Vector &heap_locations, const idx_t col_idx,
                                             const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	UnifiedVectorFormat source_data;
	source.ToUnifiedFormat(source_count, source_data);
	const auto source_sel = *source_data.sel;
	const auto &source_validity = source_data.validity;

	// List data
	const auto list_sel = *list_data.sel;
	const auto list_entries = (list_entry_t *)list_data.data;
	const auto &list_validity = list_data.validity;

	// Target
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Initialize the validity of the STRUCTs
	for (idx_t i = 0; i < append_count; i++) {
		auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(source_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[source_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Initialize validity mask and skip heap pointer over it
		ValidityBytes child_mask(target_heap_locations[i]);
		child_mask.SetAllValid(list_length);
		target_heap_locations[i] += ValidityBytes::SizeInBytes(list_length);

		// Store the validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			auto child_source_idx = source_sel.get_index(list_offset + child_i);
			if (!source_validity.RowIsValid(child_source_idx)) {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}
	}

	// Recurse through the children
	auto &struct_sources = StructVector::GetEntries(source);
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
		const auto &struct_scatter_function = child_functions[col_idx];
		struct_scatter_function.function(*struct_sources[struct_col_idx], list_data, append_sel, append_count,
		                                 source_count, layout, row_locations, heap_locations, struct_col_idx,
		                                 struct_scatter_function.child_functions);
	}
}

static void ListWithinListTupleDataScatter(Vector &child_list, const UnifiedVectorFormat &list_data,
                                           const SelectionVector &append_sel, const idx_t append_count,
                                           const idx_t child_list_count, const TupleDataLayout &layout,
                                           Vector &row_locations, Vector &heap_locations, const idx_t col_idx,
                                           const vector<TupleDataScatterFunction> &child_functions) {
	// List data (of the list Vector that "source" is in)
	const auto list_sel = *list_data.sel;
	const auto list_entries = (list_entry_t *)list_data.data;
	const auto &list_validity = list_data.validity;

	// Child list
	UnifiedVectorFormat child_list_data;
	child_list.ToUnifiedFormat(child_list_count, child_list_data);
	const auto child_list_sel = *child_list_data.sel;
	const auto child_list_entries = (list_entry_t *)child_list_data.data;
	const auto &child_list_validity = child_list_data.validity;

	// Target
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Child list child
	auto &child_list_child_vec = ListVector::GetEntry(child_list);
	const auto child_list_child_count = ListVector::GetListSize(child_list);
	UnifiedVectorFormat child_list_child_data;
	child_list_child_vec.ToUnifiedFormat(child_list_child_count, child_list_child_data);
	const auto &child_list_child_sel = *child_list_data.sel;

	// Construct combined list entries and a selection vector for the child list child
	// TODO: not necessary if everything's flat?
	list_entry_t combined_list_entries[STANDARD_VECTOR_SIZE];
	SelectionVector combined_sel(child_list_child_count);
	idx_t combined_list_offset = 0;

	for (idx_t i = 0; i < append_count; i++) {
		auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child list
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Initialize validity mask and skip heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto child_data_location = target_heap_location;
		target_heap_location += list_entry.length * TupleDataWithinListFixedSize<list_entry_t>();

		idx_t child_list_size = 0;
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			auto child_list_idx = child_list_sel.get_index(list_offset + child_i);
			if (child_list_validity.RowIsValid(child_list_idx)) {
				// Get the child list entry
				const auto &child_list_entry = child_list_entries[child_list_idx];
				const auto &child_list_offset = child_list_entry.offset;
				const auto &child_list_length = child_list_entry.length;

				// Store the child list entries
				TupleDataWithinListValueStore<list_entry_t>(
				    child_list_entry, child_data_location + child_i * TupleDataWithinListFixedSize<list_entry_t>(),
				    target_heap_location);

				// Combine the selection vector of the child list with that of the child list child
				for (idx_t child_child_i = 0; child_child_i < child_list_length; child_child_i++) {
					combined_sel.set_index(child_list_child_sel.get_index(child_list_offset + child_child_i),
					                       combined_list_offset + child_list_size + child_child_i);
				}

				child_list_size += child_list_entry.length;
			} else {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}

		// Combine the child list entries into one
		combined_list_entries[list_idx] = {combined_list_offset, child_list_size};
		combined_list_offset += child_list_size;
	}

	// Slice the child list child using the combined selection vector
	Vector sliced_child_list_child_vec(child_list_child_vec);
	sliced_child_list_child_vec.Slice(combined_sel, child_list_child_count);

	// Modify child_list_data to be used as list_data in the recursion
	child_list_data.sel = list_data.sel;
	child_list_data.data = (data_ptr_t)combined_list_entries;
	child_list_data.validity = list_data.validity;

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	const auto &child_function = child_functions[0];
	child_function.function(child_list_child_vec, child_list_data, append_sel, append_count, child_list_child_count,
	                        layout, row_locations, heap_locations, col_idx, child_function.child_functions);
}

template <class T>
tuple_data_scatter_function_t GetTupleDataScatterFunction(bool within_list) {
	return within_list ? TemplatedWithinListTupleDataScatter<T> : TemplatedTupleDataScatter<T>;
}

TupleDataScatterFunction TupleDataCollection::GetScatterFunction(LogicalType type, const TupleDataLayout &layout,
                                                                 idx_t col_idx, bool within_list) {
	TupleDataScatterFunction result;
	tuple_data_scatter_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = GetTupleDataScatterFunction<bool>(within_list);
		break;
	case PhysicalType::INT8:
		function = GetTupleDataScatterFunction<int8_t>(within_list);
		break;
	case PhysicalType::INT16:
		function = GetTupleDataScatterFunction<int16_t>(within_list);
		break;
	case PhysicalType::INT32:
		function = GetTupleDataScatterFunction<int32_t>(within_list);
		break;
	case PhysicalType::INT64:
		function = GetTupleDataScatterFunction<int64_t>(within_list);
		break;
	case PhysicalType::INT128:
		function = GetTupleDataScatterFunction<hugeint_t>(within_list);
		break;
	case PhysicalType::UINT8:
		function = GetTupleDataScatterFunction<uint8_t>(within_list);
		break;
	case PhysicalType::UINT16:
		function = GetTupleDataScatterFunction<uint16_t>(within_list);
		break;
	case PhysicalType::UINT32:
		function = GetTupleDataScatterFunction<uint32_t>(within_list);
		break;
	case PhysicalType::UINT64:
		function = GetTupleDataScatterFunction<uint64_t>(within_list);
		break;
	case PhysicalType::FLOAT:
		function = GetTupleDataScatterFunction<float>(within_list);
		break;
	case PhysicalType::DOUBLE:
		function = GetTupleDataScatterFunction<double>(within_list);
		break;
	case PhysicalType::INTERVAL:
		function = GetTupleDataScatterFunction<interval_t>(within_list);
		break;
	case PhysicalType::VARCHAR:
		function = GetTupleDataScatterFunction<string_t>(within_list);
		break;
	case PhysicalType::STRUCT: {
		function = within_list ? StructWithinListTupleDataScatter : StructTupleDataScatter;
		D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
		const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
			result.child_functions.emplace_back(GetScatterFunction(StructType::GetChildType(type, struct_col_idx),
			                                                       struct_layout, struct_col_idx, within_list));
		}
		break;
	}
	case PhysicalType::LIST:
		function = within_list ? ListWithinListTupleDataScatter : ListTupleDataScatter;
		result.child_functions.emplace_back(GetScatterFunction(ListType::GetChildType(type), layout, col_idx, true));
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetScatterFunction");
	}
	result.function = function;
	return result;
}

void TupleDataCollection::FinalizePinState(TupleDataPinState &pin_state, TupleDataSegment &segment) {
	segment.allocator->ReleaseOrStoreHandles(pin_state, segment);
}

void TupleDataCollection::FinalizePinState(TupleDataPinState &pin_state) {
	D_ASSERT(segments.size() == 1);
	allocator->ReleaseOrStoreHandles(pin_state, segments.back());
}

bool TupleDataCollection::NextScanIndex(TupleDataScanState &state, idx_t &segment_index, idx_t &chunk_index) {
	// Check if we still have segments to scan
	if (state.segment_index >= segments.size()) {
		// No more data left in the scan
		return false;
	}
	// Check within the current segment if we still have chunks to scan
	while (state.chunk_index >= segments[state.segment_index].ChunkCount()) {
		// Exhausted all chunks for this segment: Move to the next one
		FinalizePinState(state.pin_state, segments[state.segment_index]);
		state.segment_index++;
		state.chunk_index = 0;
		if (state.segment_index >= segments.size()) {
			return false;
		}
	}
	segment_index = state.segment_index;
	chunk_index = state.chunk_index++;
	return true;
}

void TupleDataCollection::ScanAtIndex(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                      const vector<column_t> &column_ids, idx_t segment_index, idx_t chunk_index,
                                      DataChunk &result) {
	auto &segment = segments[segment_index];
	auto &chunk = segment.chunks[chunk_index];
	segment.allocator->InitializeChunkState(segment, pin_state, chunk_state, chunk_index, false);
	result.Reset();
	Gather(chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(), chunk.count, column_ids, result,
	       *FlatVector::IncrementalSelectionVector());
	result.SetCardinality(chunk.count);
}

template <class T>
static void TemplatedTupleDataGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                     const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                     const SelectionVector &target_sel,
                                     const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

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
		ValidityBytes row_mask(source_row);
		if (row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			target_data[target_idx] = Load<T>(source_row + offset_in_row);
		} else {
			target_validity.SetInvalid(target_idx);
		}
	}
}

static void StructTupleDataGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                  const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                  const SelectionVector &target_sel,
                                  const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

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
		ValidityBytes row_mask(source_row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			const auto target_idx = target_sel.get_index(i);
			target_validity.SetInvalid(target_idx);
		}

		// Set the pointer
		struct_source_locations[source_idx] = source_row + offset_in_row;
	}

	// Get the struct layout and struct entries
	D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
	const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
	auto &struct_targets = StructVector::GetEntries(target);
	D_ASSERT(struct_layout.ColumnCount() == struct_targets.size());

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		const auto &struct_target = struct_targets[struct_col_idx];
		const auto &struct_gather_function = child_functions[col_idx];
		struct_gather_function.function(struct_layout, struct_row_locations, struct_col_idx, scan_sel, scan_count,
		                                *struct_target, target_sel, struct_gather_function.child_functions);
	}
}

static void ListTupleDataGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                const SelectionVector &target_sel,
                                const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto target_list_entries = FlatVector::GetData<list_entry_t>(target);
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Load pointers to the data from the row
	Vector heap_locations(LogicalType::POINTER);
	auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);
	auto &source_heap_validity = FlatVector::Validity(heap_locations);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	uint64_t target_list_offset = 0;
	for (idx_t i = 0; i < scan_count; i++) {
		auto source_idx = scan_sel.get_index(i);
		auto target_idx = target_sel.get_index(i);

		const auto &source_row = source_locations[source_idx];
		ValidityBytes row_mask(source_row);
		if (row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			auto &source_heap_location = source_heap_locations[source_idx];
			source_heap_location = Load<data_ptr_t>(source_row + offset_in_row);

			// Load list size, initialize list entry, and increment offset
			auto list_length = Load<uint64_t>(source_heap_location);
			target_list_entries[target_idx] = {target_list_offset, list_length};
			target_list_offset += list_length;
		} else {
			source_heap_validity.SetInvalid(source_idx);
			target_validity.SetInvalid(target_idx);
		}
	}
	ListVector::Reserve(target, target_list_offset);
	ListVector::SetListSize(target, target_list_offset);

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	const auto &child_function = child_functions[0];
	child_function.function(layout, heap_locations, col_idx, scan_sel, scan_count, ListVector::GetEntry(target),
	                        target_sel, child_function.child_functions);
}

template <class T>
static void TemplatedWithinListTupleDataGather(const TupleDataLayout &layout, Vector &heap_locations,
                                               const idx_t col_idx, const SelectionVector &scan_sel,
                                               const idx_t scan_count, Vector &target,
                                               const SelectionVector &target_sel,
                                               const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);
	auto &source_heap_validity = FlatVector::Validity(heap_locations);

	// Target
	auto target_data = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	uint64_t target_offset = 0;
	for (idx_t i = 0; i < scan_count; i++) {
		auto source_idx = scan_sel.get_index(i);
		if (!source_heap_validity.RowIsValid(source_idx)) {
			continue;
		}

		// Load list size and skip over
		auto &source_heap_location = source_heap_locations[source_idx];
		const auto list_length = Load<uint64_t>(source_heap_location);
		source_heap_location += sizeof(uint64_t);

		// Initialize validity mask
		ValidityBytes source_mask(source_heap_location);
		source_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto source_data_location = source_heap_location;
		source_heap_location += list_length * TupleDataWithinListFixedSize<T>();

		// Load the child validity and data belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			if (source_mask.RowIsValidUnsafe(child_i)) {
				target_data[target_offset + child_i] = TupleDataWithinListValueLoad<T>(
				    source_data_location + child_i * TupleDataWithinListFixedSize<T>(), source_heap_location);
			} else {
				target_validity.SetInvalid(target_offset + child_i);
			}
		}
		target_offset += list_length;
	}
}

static void StructWithinListTupleDataGather(const TupleDataLayout &layout, Vector &heap_locations, const idx_t col_idx,
                                            const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                            const SelectionVector &target_sel,
                                            const vector<TupleDataGatherFunction> &child_functions) {
}

static void ListWithinListTupleDataGather(const TupleDataLayout &layout, Vector &heap_locations, const idx_t col_idx,
                                          const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                          const SelectionVector &target_sel,
                                          const vector<TupleDataGatherFunction> &child_functions) {
}

template <class T>
tuple_data_gather_function_t GetTupleDataGatherFunction(bool within_list) {
	return within_list ? TemplatedWithinListTupleDataGather<T> : TemplatedTupleDataGather<T>;
}

TupleDataGatherFunction TupleDataCollection::GetGatherFunction(LogicalType type, const TupleDataLayout &layout,
                                                               idx_t col_idx, bool within_list) {
	TupleDataGatherFunction result;
	tuple_data_gather_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = GetTupleDataGatherFunction<bool>(within_list);
		break;
	case PhysicalType::INT8:
		function = GetTupleDataGatherFunction<int8_t>(within_list);
		break;
	case PhysicalType::INT16:
		function = GetTupleDataGatherFunction<int16_t>(within_list);
		break;
	case PhysicalType::INT32:
		function = GetTupleDataGatherFunction<int32_t>(within_list);
		break;
	case PhysicalType::INT64:
		function = GetTupleDataGatherFunction<int64_t>(within_list);
		break;
	case PhysicalType::INT128:
		function = GetTupleDataGatherFunction<hugeint_t>(within_list);
		break;
	case PhysicalType::UINT8:
		function = GetTupleDataGatherFunction<uint8_t>(within_list);
		break;
	case PhysicalType::UINT16:
		function = GetTupleDataGatherFunction<uint16_t>(within_list);
		break;
	case PhysicalType::UINT32:
		function = GetTupleDataGatherFunction<uint32_t>(within_list);
		break;
	case PhysicalType::UINT64:
		function = GetTupleDataGatherFunction<uint64_t>(within_list);
		break;
	case PhysicalType::FLOAT:
		function = GetTupleDataGatherFunction<float>(within_list);
		break;
	case PhysicalType::DOUBLE:
		function = GetTupleDataGatherFunction<double>(within_list);
		break;
	case PhysicalType::INTERVAL:
		function = GetTupleDataGatherFunction<interval_t>(within_list);
		break;
	case PhysicalType::VARCHAR:
		function = GetTupleDataGatherFunction<string_t>(within_list);
		break;
	case PhysicalType::STRUCT: {
		function = within_list ? StructWithinListTupleDataGather : StructTupleDataGather;
		D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
		const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
			result.child_functions.push_back(GetGatherFunction(StructType::GetChildType(type, struct_col_idx),
			                                                   struct_layout, struct_col_idx, within_list));
		}
		break;
	}
	case PhysicalType::LIST:
		function = within_list ? ListWithinListTupleDataGather : ListTupleDataGather;
		result.child_functions.push_back(GetGatherFunction(ListType::GetChildType(type), layout, col_idx, true));
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetGatherFunction");
	}
	result.function = function;
	return result;
}

string TupleDataCollection::ToString() {
	DataChunk chunk;
	InitializeChunk(chunk);

	TupleDataScanState scan_state;
	InitializeScan(scan_state);

	string result = StringUtil::Format("TupleDataCollection - [%llu Chunks, %llu Rows]\n", ChunkCount(), Count());
	idx_t chunk_idx = 0;
	idx_t row_count = 0;
	while (Scan(scan_state, chunk)) {
		result +=
		    StringUtil::Format("Chunk %llu - [Rows %llu - %llu]\n", chunk_idx, row_count, row_count + chunk.size()) +
		    chunk.ToString();
		chunk_idx++;
		row_count += chunk.size();
	}

	return result;
}

void TupleDataCollection::Print() {
	Printer::Print(ToString());
}

void TupleDataCollection::Verify() const {
#ifdef DEBUG
	idx_t total_segment_count = 0;
	for (auto &segment : segments) {
		segment.Verify();
		total_segment_count += segment.count;
	}
	D_ASSERT(total_segment_count == this->count);
#endif
}

} // namespace duckdb
