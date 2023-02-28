#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_allocator.hpp"

#include <algorithm>

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

typedef void (*tuple_data_scatter_function_t)(Vector &source, const UnifiedVectorFormat &source_data,
                                              const idx_t source_offset, const idx_t count,
                                              const TupleDataLayout &layout, Vector &row_locations,
                                              Vector &heap_locations, const SelectionVector &row_sel,
                                              const idx_t col_idx,
                                              const vector<TupleDataScatterFunction> &child_functions);

struct TupleDataScatterFunction {
	tuple_data_scatter_function_t function;
	vector<TupleDataScatterFunction> child_functions;
};

typedef void (*tuple_data_gather_function_t)(Vector &row_locations, const SelectionVector &sel,
                                             const TupleDataLayout &layout, const idx_t col_idx, const idx_t count,
                                             Vector &target, const vector<TupleDataGatherFunction> &child_functions);

struct TupleDataGatherFunction {
	tuple_data_gather_function_t function;
	vector<TupleDataGatherFunction> child_functions;
};

TupleDataCollection::TupleDataCollection(ClientContext &context, TupleDataLayout layout_p)
    : layout(std::move(layout_p)), allocator(make_shared<TupleDataAllocator>(context, layout)) {
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

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, TupleDataAppendProperties properties) {
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}
	InitializeAppend(append_state, std::move(column_ids), properties);
}

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids,
                                           TupleDataAppendProperties properties) {
	VerifyAppendColumns(layout, column_ids);
	append_state.vector_data.resize(layout.ColumnCount());
	append_state.properties = properties;
	append_state.column_ids = std::move(column_ids);
	if (segments.empty()) {
		segments.emplace_back(allocator);
	}
}

void TupleDataCollection::Append(DataChunk &new_chunk) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state);
	Append(append_state, new_chunk);
}

void TupleDataCollection::Append(DataChunk &new_chunk, vector<column_t> column_ids) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state, std::move(column_ids));
	Append(append_state, new_chunk);
}

void TupleDataCollection::Append(TupleDataAppendState &append_state, DataChunk &new_chunk) {
	D_ASSERT(segments.size() == 1);                                    // Cannot append after Combine
	D_ASSERT(append_state.vector_data.size() == layout.ColumnCount()); // Needs InitializeAppend
	for (const auto &col_idx : append_state.column_ids) {
		new_chunk.data[col_idx].ToUnifiedFormat(new_chunk.size(), append_state.vector_data[col_idx]);
	}
	if (!layout.AllConstant()) {
		ComputeHeapSizes(append_state, new_chunk);
	}

	allocator->Build(append_state, count, segments.back());

	// Set the validity mask for each row before inserting data
	auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.chunk_state.row_locations);
	for (idx_t i = 0; i < count; ++i) {
		ValidityBytes(row_locations[i]).SetAllValid(layout.ColumnCount());
	}

	// Write the data
	for (const auto &col_idx : append_state.column_ids) {
		Scatter(append_state, new_chunk.data[col_idx], col_idx, new_chunk.size(),
		        *FlatVector::IncrementalSelectionVector());
	}
}

void TupleDataCollection::Scatter(TupleDataAppendState &append_state, Vector &source, const column_t column_id,
                                  const idx_t append_count, const SelectionVector &sel) {
	const auto &scatter_function = scatter_functions[column_id];
	scatter_function.function(source, append_state.vector_data[column_id], 0, append_count, layout,
	                          append_state.chunk_state.row_locations, append_state.chunk_state.heap_locations, sel,
	                          column_id, scatter_function.child_functions);
}

void TupleDataCollection::Combine(TupleDataCollection &other) {
	if (other.count == 0) {
		return;
	}
	if (layout.GetTypes() != other.GetLayout().GetTypes()) {
		throw InternalException("Attempting to combine TupleDataCollection with mismatching types");
	}
	this->count += other.count;
	this->segments.reserve(segments.size() + other.segments.size());
	for (auto &other_seg : other.segments) {
		segments.push_back(std::move(other_seg));
	}
	Verify();
}

void TupleDataCollection::Unpin() {
	for (auto &segment : segments) {
		segment.pinned_handles.clear();
	}
}

void TupleDataCollection::InitializeScanChunk(DataChunk &chunk) const {
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
	state.chunk_state.row_handles.clear();
	state.chunk_state.heap_handles.clear();
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
	if (segment_index_before != segment_index) {
		state.chunk_state.row_handles.clear();
		state.chunk_state.heap_handles.clear();
	}
	ScanAtIndex(state.chunk_state, state.column_ids, segment_index, chunk_index, result);
	return true;
}

bool TupleDataCollection::Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result) {
	idx_t segment_index;
	idx_t chunk_index;
	{
		lock_guard<mutex> guard(gstate.lock);
		if (!NextScanIndex(gstate.scan_state, segment_index, chunk_index)) {
			return false;
		}
	}
	if (lstate.segment_index != segment_index) {
		lstate.chunk_state.row_handles.clear();
		lstate.chunk_state.heap_handles.clear();
		lstate.segment_index = segment_index;
	}
	ScanAtIndex(lstate.chunk_state, gstate.scan_state.column_ids, segment_index, chunk_index, result);
	return true;
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &sel, const vector<column_t> &column_ids,
                                 const idx_t scan_count, DataChunk &result) const {
	D_ASSERT(column_ids.size() == result.ColumnCount());
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		Gather(row_locations, sel, column_ids[col_idx], scan_count, result.data[col_idx]);
	}
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &sel, const column_t column_id,
                                 const idx_t scan_count, Vector &result) const {
	const auto &gather_function = gather_functions[column_id];
	gather_function.function(row_locations, sel, layout, column_id, scan_count, result,
	                         gather_function.child_functions);
}

void TupleDataCollection::ComputeHeapSizes(TupleDataAppendState &append_state, DataChunk &new_chunk) {
	auto heap_sizes = FlatVector::GetData<idx_t>(append_state.chunk_state.heap_sizes);
	std::fill_n(heap_sizes, new_chunk.size(), 0);

	for (idx_t col_idx = 0; col_idx < new_chunk.ColumnCount(); col_idx++) {
		ComputeHeapSizes(append_state.chunk_state.heap_sizes, new_chunk.data[col_idx],
		                 append_state.vector_data[col_idx], new_chunk.size());
	}
}

void TupleDataCollection::ComputeHeapSizes(Vector &heap_sizes_v, Vector &source_v, UnifiedVectorFormat &source,
                                           const idx_t count) {
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);
	const auto &source_sel = *source.sel;
	const auto &source_validity = source.validity;

	switch (source_v.GetType().InternalType()) {
	case PhysicalType::VARCHAR: {
		const auto source_data = (string_t *)source.data;
		for (idx_t i = 0; i < count; i++) {
			const auto idx = source_sel.get_index(i);
			if (source_validity.RowIsValid(idx) && !source_data[idx].IsInlined()) {
				heap_sizes[i] += source_data[idx].GetSize();
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		auto &struct_sources = StructVector::GetEntries(source_v);

		// Recurse through the struct children
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
			const auto &struct_source = struct_sources[struct_col_idx];
			UnifiedVectorFormat struct_source_data;
			struct_source->ToUnifiedFormat(count, struct_source_data);
			ComputeHeapSizes(heap_sizes_v, *struct_source, struct_source_data, count);
		}
		break;
	}
	case PhysicalType::LIST: {
		RowOperations::ComputeEntrySizes(source_v, source, heap_sizes, count, count,
		                                 *FlatVector::IncrementalSelectionVector());
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
static void TemplatedTupleDataScatter(Vector &source, const UnifiedVectorFormat &source_data, const idx_t source_offset,
                                      const idx_t count, const TupleDataLayout &layout, Vector &row_locations,
                                      Vector &heap_locations, const SelectionVector &row_sel, const idx_t col_idx,
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
		for (idx_t i = 0; i < count; i++) {
			auto idx = row_sel.get_index(i + source_offset);
			auto source_idx = source_sel.get_index(idx);
			TupleDataValueScatter<T>(data[source_idx], target_locations[idx], offset_in_row,
			                         target_heap_locations[idx]);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = row_sel.get_index(i + source_offset);
			auto source_idx = source_sel.get_index(idx);
			if (validity.RowIsValid(source_idx)) {
				TupleDataValueScatter<T>(data[source_idx], target_locations[idx], offset_in_row,
				                         target_heap_locations[idx]);
			} else {
				TupleDataValueScatter<T>(data[source_idx], target_locations[idx], offset_in_row,
				                         target_heap_locations[idx]);
				ValidityBytes(target_locations[idx]).SetValidUnsafe(col_idx);
			}
		}
	}
}

static void StructTupleDataScatter(Vector &source, const UnifiedVectorFormat &source_data, const idx_t source_offset,
                                   const idx_t count, const TupleDataLayout &layout, Vector &row_locations,
                                   Vector &heap_locations, const SelectionVector &row_sel, const idx_t col_idx,
                                   const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto source_sel = *source_data.sel;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Set validity of the struct
	for (idx_t i = 0; i < count; i++) {
		auto idx = row_sel.get_index(i + source_offset);
		auto source_idx = source_sel.get_index(idx);
		if (!validity.RowIsValid(source_idx)) {
			ValidityBytes(target_locations[idx]).SetValidUnsafe(col_idx);
		}
	}

	// Create a Vector of pointers pointing to the start of the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER, count);
	auto struct_target_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < count; i++) {
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
		struct_source->ToUnifiedFormat(count, struct_source_data);
		const auto &struct_scatter_function = child_functions[col_idx];
		struct_scatter_function.function(*struct_sources[struct_col_idx], struct_source_data, source_offset, count,
		                                 struct_layout, struct_row_locations, heap_locations, row_sel, struct_col_idx,
		                                 struct_scatter_function.child_functions);
	}
}

static void ListTupleDataScatter(Vector &source, const UnifiedVectorFormat &source_data, const idx_t source_offset,
                                 const idx_t count, const TupleDataLayout &layout, Vector &row_locations,
                                 Vector &heap_locations, const SelectionVector &row_sel, const idx_t col_idx,
                                 const vector<TupleDataScatterFunction> &child_functions) {
	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// TODO: re-write the list scatter at some point
	// TODO: also the parameters passed here are probably wrong
	RowOperations::HeapScatter(source, count, row_sel, count, col_idx, target_locations, target_locations,
	                           source_offset);
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

bool TupleDataCollection::NextScanIndex(TupleDataScanState &state, idx_t &segment_index, idx_t &chunk_index) const {
	// Check if we still have segments to scan
	if (state.segment_index >= segments.size()) {
		// No more data left in the scan
		return false;
	}
	// check within the current segment if we still have chunks to scan
	while (state.chunk_index >= segments[state.segment_index].ChunkCount()) {
		// Exhausted all chunks for this segment: Move to the next one
		state.chunk_state.row_handles.clear();
		state.chunk_state.heap_handles.clear();
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

void TupleDataCollection::ScanAtIndex(TupleDataManagementState &chunk_state, const vector<column_t> &column_ids,
                                      idx_t segment_index, idx_t chunk_index, DataChunk &result) {
	auto &segment = segments[segment_index];
	auto &chunk = segment.chunks[chunk_index];
	segment.allocator->InitializeChunkState(chunk_state, chunk);
	Gather(chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(), column_ids, chunk.count, result);
}

template <class T>
static void TemplatedTupleDataGather(Vector &row_locations, const SelectionVector &sel, const TupleDataLayout &layout,
                                     const idx_t col_idx, const idx_t count, Vector &target,
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

	for (idx_t i = 0; i < count; i++) {
		const auto &source_location = source_locations[sel.get_index(i)];
		ValidityBytes row_mask(source_location);
		if (row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			target_data[i] = Load<T>(source_location + offset_in_row);
		} else {
			target_validity.SetInvalid(i);
		}
	}
}

static void StructTupleDataGather(Vector &row_locations, const SelectionVector &sel, const TupleDataLayout &layout,
                                  const idx_t col_idx, const idx_t count, Vector &target,
                                  const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Get validity of the struct
	for (idx_t i = 0; i < count; i++) {
		const auto &source_location = source_locations[sel.get_index(i)];
		ValidityBytes row_mask(source_location);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			target_validity.SetInvalid(i);
		}
	}

	// Create a Vector of pointers pointing to the start of the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER, count);
	auto struct_source_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < count; i++) {
		const auto &source_location = source_locations[sel.get_index(i)];
		struct_source_locations[i] = source_location + offset_in_row;
	}

	D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
	const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
	auto &struct_targets = StructVector::GetEntries(target);
	D_ASSERT(struct_layout.ColumnCount() == struct_targets.size());

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		const auto &struct_target = struct_targets[struct_col_idx];
		const auto &struct_gather_function = child_functions[col_idx];
		// We pass an incremental selection vector instead of 'sel' because of how we built struct_source_locations
		struct_gather_function.function(struct_row_locations, *FlatVector::IncrementalSelectionVector(), struct_layout,
		                                struct_col_idx, count, *struct_target, struct_gather_function.child_functions);
	}
}

static void ListTupleDataGather(Vector &row_locations, const SelectionVector &sel, const TupleDataLayout &layout,
                                const idx_t col_idx, const idx_t count, Vector &target,
                                const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// TODO: re-write the list gather at some point
	// TODO: also the parameters passed here are probably wrong
	RowOperations::HeapGather(target, count, sel, col_idx, source_locations, source_locations);
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
