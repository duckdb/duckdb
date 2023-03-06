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

TupleDataCollection::TupleDataCollection(BufferManager &buffer_manager, TupleDataLayout layout_p)
    : layout(std::move(layout_p)), allocator(make_shared<TupleDataAllocator>(buffer_manager, layout)) {
	Initialize();
}

TupleDataCollection::TupleDataCollection(shared_ptr<TupleDataAllocator> allocator) {
	this->layout = allocator->GetLayout();
	this->allocator = std::move(allocator);
	Initialize();
}

TupleDataCollection::~TupleDataCollection() {
}

void TupleDataCollection::Initialize() {
	D_ASSERT(!layout.GetTypes().empty());
	this->count = 0;
	scatter_functions.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		scatter_functions.emplace_back(GetScatterFunction(layout, col_idx));
	}
	gather_functions.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		gather_functions.emplace_back(GetGatherFunction(layout, col_idx));
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
	append_state.vector_data.resize(layout.ColumnCount());
	append_state.pin_state.properties = properties;
	append_state.column_ids = std::move(column_ids);
	if (segments.empty()) {
		segments.emplace_back(allocator);
	}
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
	D_ASSERT(append_state.vector_data.size() == layout.ColumnCount()); // Needs InitializeAppend
	if (append_count == DConstants::INVALID_INDEX) {
		append_count = new_chunk.size();
	}

	for (const auto &col_idx : append_state.column_ids) {
		new_chunk.data[col_idx].ToUnifiedFormat(new_chunk.size(), append_state.vector_data[col_idx]);
	}
	if (!layout.AllConstant()) {
		ComputeHeapSizes(append_state, new_chunk, append_sel, append_count);
	}

	segments.back().allocator->Build(segments.back(), append_state.pin_state, append_state.chunk_state, 0,
	                                 append_count);

	// Set the validity mask for each row before inserting data
	auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.chunk_state.row_locations);
	for (idx_t i = 0; i < append_count; i++) {
		ValidityBytes(row_locations[i]).SetAllValid(layout.ColumnCount());
	}

	if (!layout.AllConstant()) {
		// Set the heap size for each row
		const auto heap_size_offset = layout.GetHeapSizeOffset();
		const auto heap_sizes = FlatVector::GetData<idx_t>(append_state.chunk_state.heap_sizes);
		for (idx_t i = 0; i < append_count; i++) {
			Store<uint32_t>(heap_sizes[i], row_locations[i] + heap_size_offset);
		}
	}

	// Write the data
	for (const auto &col_idx : append_state.column_ids) {
		Scatter(append_state, new_chunk.data[col_idx], col_idx, append_sel, append_count, new_chunk.size());
	}

	count += append_count;
	Verify();
}

void TupleDataCollection::Scatter(TupleDataAppendState &append_state, Vector &source, const column_t column_id,
                                  const SelectionVector &append_sel, const idx_t append_count,
                                  const idx_t original_count) {
	const auto &scatter_function = scatter_functions[column_id];
	scatter_function.function(source, append_state.vector_data[column_id], append_sel, append_count, original_count,
	                          layout, append_state.chunk_state.row_locations, append_state.chunk_state.heap_locations,
	                          column_id, scatter_function.child_functions);
}

void TupleDataCollection::FinalizeAppendState(TupleDataAppendState &append_state) {
	FinalizeChunkState(append_state.pin_state);
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
	other.segments.clear();
	other.count = 0;
	Verify();
}

void TupleDataCollection::InitializeChunk(DataChunk &chunk) const {
	chunk.Initialize(allocator->GetAllocator(), layout.GetTypes());
}

void TupleDataCollection::InitializeScanChunk(TupleDataScanState &state, DataChunk &chunk) const {
	D_ASSERT(!state.column_ids.empty());
	vector<LogicalType> chunk_types;
	chunk_types.reserve(state.column_ids.size());
	for (idx_t i = 0; i < state.column_ids.size(); i++) {
		auto column_idx = state.column_ids[i];
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
	state.segment_index = 0;
	state.chunk_index = 0;
	state.properties = properties;
	state.column_ids = std::move(column_ids);
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
	ScanAtIndex(state.pin_state, state.chunk_state, state.column_ids, segment_index, chunk_index, result);
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
	ScanAtIndex(scan_state.pin_state, scan_state.chunk_state, gstate.scan_state.column_ids, scan_state.segment_index,
	            scan_state.chunk_index, result);
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
		Gather(row_locations, scan_sel, column_ids[col_idx], scan_count, result.data[col_idx], target_sel);
	}
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 const column_t column_id, Vector &result, const SelectionVector &target_sel) const {
	const auto &gather_function = gather_functions[column_id];
	gather_function.function(layout, row_locations, column_id, scan_sel, scan_count, result, target_sel,
	                         gather_function.child_functions);
}

void TupleDataCollection::ComputeHeapSizes(TupleDataAppendState &append_state, DataChunk &new_chunk,
                                           const SelectionVector &append_sel, const idx_t append_count) {
	auto heap_sizes = FlatVector::GetData<idx_t>(append_state.chunk_state.heap_sizes);
	std::fill_n(heap_sizes, new_chunk.size(), 0);

	for (idx_t col_idx = 0; col_idx < new_chunk.ColumnCount(); col_idx++) {
		ComputeHeapSizes(append_state.chunk_state.heap_sizes, new_chunk.data[col_idx],
		                 append_state.vector_data[col_idx], append_sel, append_count, new_chunk.size());
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

template <class T>
static inline void TupleDataValueScatter(const T &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                         data_ptr_t &heap_location) {
	Store<T>(source, row_location + offset_in_row);
}

template <>
inline void TupleDataValueScatter(const string_t &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                  data_ptr_t &heap_location) {
	memcpy(heap_location, source.GetDataUnsafe(), source.GetSize());
	Store<string_t>(string_t((const char *)heap_location, source.GetSize()), row_location + offset_in_row);
	heap_location += source.GetSize();
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

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	if (validity.AllValid()) {
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_sel.get_index(append_sel.get_index(i));
			TupleDataValueScatter<T>(data[source_idx], target_locations[i], offset_in_row, target_heap_locations[i]);
		}
	} else {
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (validity.RowIsValid(source_idx)) {
				TupleDataValueScatter<T>(data[source_idx], target_locations[i], offset_in_row,
				                         target_heap_locations[i]);
			} else {
				TupleDataValueScatter<T>(data[source_idx], target_locations[i], offset_in_row,
				                         target_heap_locations[i]);
				ValidityBytes(target_locations[i]).SetValidUnsafe(col_idx);
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

	// Set validity of the STRUCT in this layout
	for (idx_t i = 0; i < append_count; i++) {
		auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (!validity.RowIsValid(source_idx)) {
			ValidityBytes(target_locations[i]).SetValidUnsafe(col_idx);
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
	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Store pointers to the data in the rows
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		Store<data_ptr_t>(target_heap_locations[i], target_locations[i] + offset_in_row);
	}

	// TODO: re-write the list scatter at some point
	RowOperations::HeapScatter(source, original_count, append_sel, append_count, col_idx, target_heap_locations,
	                           target_locations);
}

TupleDataScatterFunction TupleDataCollection::GetScatterFunction(const TupleDataLayout &layout, idx_t col_idx) {
	const auto &type = layout.GetTypes()[col_idx];

	TupleDataScatterFunction result;
	tuple_data_scatter_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = TemplatedTupleDataScatter<bool>;
		break;
	case PhysicalType::INT8:
		function = TemplatedTupleDataScatter<int8_t>;
		break;
	case PhysicalType::INT16:
		function = TemplatedTupleDataScatter<int16_t>;
		break;
	case PhysicalType::INT32:
		function = TemplatedTupleDataScatter<int32_t>;
		break;
	case PhysicalType::INT64:
		function = TemplatedTupleDataScatter<int64_t>;
		break;
	case PhysicalType::INT128:
		function = TemplatedTupleDataScatter<hugeint_t>;
		break;
	case PhysicalType::UINT8:
		function = TemplatedTupleDataScatter<uint8_t>;
		break;
	case PhysicalType::UINT16:
		function = TemplatedTupleDataScatter<uint16_t>;
		break;
	case PhysicalType::UINT32:
		function = TemplatedTupleDataScatter<uint32_t>;
		break;
	case PhysicalType::UINT64:
		function = TemplatedTupleDataScatter<uint64_t>;
		break;
	case PhysicalType::FLOAT:
		function = TemplatedTupleDataScatter<float>;
		break;
	case PhysicalType::DOUBLE:
		function = TemplatedTupleDataScatter<double>;
		break;
	case PhysicalType::INTERVAL:
		function = TemplatedTupleDataScatter<interval_t>;
		break;
	case PhysicalType::VARCHAR:
		function = TemplatedTupleDataScatter<string_t>;
		break;
	case PhysicalType::STRUCT: {
		function = StructTupleDataScatter;
		D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
		const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
			result.child_functions.push_back(GetScatterFunction(struct_layout, struct_col_idx));
		}
		break;
	}
	case PhysicalType::LIST:
		function = ListTupleDataScatter;
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetScatterFunction");
	}
	result.function = function;
	return result;
}

void TupleDataCollection::FinalizeChunkState(TupleDataManagementState &state) {
	allocator->ReleaseOrStoreHandles(state, segments.back());
}

bool TupleDataCollection::NextScanIndex(TupleDataScanState &state, idx_t &segment_index, idx_t &chunk_index) const {
	// Check if we still have segments to scan
	if (state.segment_index >= segments.size()) {
		// No more data left in the scan
		return false;
	}
	// Check within the current segment if we still have chunks to scan
	while (state.chunk_index >= segments[state.segment_index].ChunkCount()) {
		// Exhausted all chunks for this segment: Move to the next one
		state.pin_state.row_handles.clear();
		state.pin_state.heap_handles.clear();
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

void TupleDataCollection::ScanAtIndex(TupleDataManagementState &pin_state, TupleDataChunkState &chunk_state,
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

	// Load pointers to the data from the row
	Vector list_locations(LogicalType::POINTER);
	auto source_heap_locations = FlatVector::GetData<data_ptr_t>(list_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < scan_count; i++) {
		const auto &source_row = source_locations[scan_sel.get_index(i)];
		source_heap_locations[i] = Load<data_ptr_t>(source_row + offset_in_row);
	}

	// TODO: re-write the list gather at some point
	RowOperations::HeapGather(target, scan_count, target_sel, col_idx, source_heap_locations, source_locations);
}

TupleDataGatherFunction TupleDataCollection::GetGatherFunction(const TupleDataLayout &layout, idx_t col_idx) {
	const auto &type = layout.GetTypes()[col_idx];

	TupleDataGatherFunction result;
	tuple_data_gather_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = TemplatedTupleDataGather<bool>;
		break;
	case PhysicalType::INT8:
		function = TemplatedTupleDataGather<int8_t>;
		break;
	case PhysicalType::INT16:
		function = TemplatedTupleDataGather<int16_t>;
		break;
	case PhysicalType::INT32:
		function = TemplatedTupleDataGather<int32_t>;
		break;
	case PhysicalType::INT64:
		function = TemplatedTupleDataGather<int64_t>;
		break;
	case PhysicalType::INT128:
		function = TemplatedTupleDataGather<hugeint_t>;
		break;
	case PhysicalType::UINT8:
		function = TemplatedTupleDataGather<uint8_t>;
		break;
	case PhysicalType::UINT16:
		function = TemplatedTupleDataGather<uint16_t>;
		break;
	case PhysicalType::UINT32:
		function = TemplatedTupleDataGather<uint32_t>;
		break;
	case PhysicalType::UINT64:
		function = TemplatedTupleDataGather<uint64_t>;
		break;
	case PhysicalType::FLOAT:
		function = TemplatedTupleDataGather<float>;
		break;
	case PhysicalType::DOUBLE:
		function = TemplatedTupleDataGather<double>;
		break;
	case PhysicalType::INTERVAL:
		function = TemplatedTupleDataGather<interval_t>;
		break;
	case PhysicalType::VARCHAR:
		function = TemplatedTupleDataGather<string_t>;
		break;
	case PhysicalType::STRUCT: {
		function = StructTupleDataGather;
		D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
		const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
			result.child_functions.push_back(GetGatherFunction(struct_layout, struct_col_idx));
		}
		break;
	}
	case PhysicalType::LIST:
		function = ListTupleDataGather;
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetGatherFunction");
	}
	result.function = function;
	return result;
}

void TupleDataCollection::Pin() {
	// use TupleDataChunkIterator with KEEP_PINNED
	throw NotImplementedException("TupleDataCollection::Pin");
}

void TupleDataCollection::Unpin() {
	for (auto &segment : segments) {
		segment.pinned_handles.clear();
	}
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
