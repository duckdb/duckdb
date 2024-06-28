#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_allocator.hpp"
#include "duckdb/common/type_visitor.hpp"

#include <algorithm>

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

TupleDataCollection::TupleDataCollection(BufferManager &buffer_manager, const TupleDataLayout &layout_p)
    : layout(layout_p.Copy()), allocator(make_shared_ptr<TupleDataAllocator>(buffer_manager, layout)) {
	Initialize();
}

TupleDataCollection::TupleDataCollection(shared_ptr<TupleDataAllocator> allocator)
    : layout(allocator->GetLayout().Copy()), allocator(std::move(allocator)) {
	Initialize();
}

TupleDataCollection::~TupleDataCollection() {
}

void TupleDataCollection::Initialize() {
	D_ASSERT(!layout.GetTypes().empty());
	this->count = 0;
	this->data_size = 0;
	scatter_functions.reserve(layout.ColumnCount());
	gather_functions.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		auto &type = layout.GetTypes()[col_idx];
		scatter_functions.emplace_back(GetScatterFunction(type));
		gather_functions.emplace_back(GetGatherFunction(type));
	}
}

void GetAllColumnIDsInternal(vector<column_t> &column_ids, const idx_t column_count) {
	column_ids.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		column_ids.emplace_back(col_idx);
	}
}

void TupleDataCollection::GetAllColumnIDs(vector<column_t> &column_ids) {
	GetAllColumnIDsInternal(column_ids, layout.ColumnCount());
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

void TupleDataCollection::Unpin() {
	for (auto &segment : segments) {
		segment.Unpin();
	}
}

// LCOV_EXCL_START
void VerifyAppendColumns(const TupleDataLayout &layout, const vector<column_t> &column_ids) {
#ifdef DEBUG
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		if (std::find(column_ids.begin(), column_ids.end(), col_idx) != column_ids.end()) {
			continue;
		}
		// This column will not be appended in the first go - verify that it is fixed-size - we cannot resize heap after
		const auto physical_type = layout.GetTypes()[col_idx].InternalType();
		D_ASSERT(physical_type != PhysicalType::VARCHAR && physical_type != PhysicalType::LIST &&
		         physical_type != PhysicalType::ARRAY);
		if (physical_type == PhysicalType::STRUCT) {
			const auto &struct_layout = layout.GetStructLayout(col_idx);
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
// LCOV_EXCL_STOP

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, TupleDataPinProperties properties) {
	vector<column_t> column_ids;
	GetAllColumnIDs(column_ids);
	InitializeAppend(append_state, std::move(column_ids), properties);
}

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids,
                                           TupleDataPinProperties properties) {
	VerifyAppendColumns(layout, column_ids);
	InitializeAppend(append_state.pin_state, properties);
	InitializeChunkState(append_state.chunk_state, std::move(column_ids));
}

void TupleDataCollection::InitializeAppend(TupleDataPinState &pin_state, TupleDataPinProperties properties) {
	pin_state.properties = properties;
	if (segments.empty()) {
		segments.emplace_back(allocator);
	}
}

static void InitializeVectorFormat(vector<TupleDataVectorFormat> &vector_data, const vector<LogicalType> &types) {
	vector_data.resize(types.size());
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		const auto &type = types[col_idx];
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			const auto &child_list = StructType::GetChildTypes(type);
			vector<LogicalType> child_types;
			child_types.reserve(child_list.size());
			for (const auto &child_entry : child_list) {
				child_types.emplace_back(child_entry.second);
			}
			InitializeVectorFormat(vector_data[col_idx].children, child_types);
			break;
		}
		case PhysicalType::LIST:
			InitializeVectorFormat(vector_data[col_idx].children, {ListType::GetChildType(type)});
			break;
		case PhysicalType::ARRAY:
			InitializeVectorFormat(vector_data[col_idx].children, {ArrayType::GetChildType(type)});
			break;
		default:
			break;
		}
	}
}

void TupleDataCollection::InitializeChunkState(TupleDataChunkState &chunk_state, vector<column_t> column_ids) {
	TupleDataCollection::InitializeChunkState(chunk_state, layout.GetTypes(), std::move(column_ids));
}

void TupleDataCollection::InitializeChunkState(TupleDataChunkState &chunk_state, const vector<LogicalType> &types,
                                               vector<column_t> column_ids) {
	if (column_ids.empty()) {
		GetAllColumnIDsInternal(column_ids, types.size());
	}
	InitializeVectorFormat(chunk_state.vector_data, types);

	for (auto &col : column_ids) {
		auto &type = types[col];
		if (TypeVisitor::Contains(type, LogicalTypeId::ARRAY)) {
			auto cast_type = ArrayType::ConvertToList(type);
			chunk_state.cached_cast_vector_cache.push_back(
			    make_uniq<VectorCache>(Allocator::DefaultAllocator(), cast_type));
			chunk_state.cached_cast_vectors.push_back(make_uniq<Vector>(*chunk_state.cached_cast_vector_cache.back()));
		} else {
			chunk_state.cached_cast_vectors.emplace_back();
			chunk_state.cached_cast_vector_cache.emplace_back();
		}
	}

	chunk_state.column_ids = std::move(column_ids);
}

void TupleDataCollection::Append(DataChunk &new_chunk, const SelectionVector &append_sel, idx_t append_count) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state);
	Append(append_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(DataChunk &new_chunk, vector<column_t> column_ids, const SelectionVector &append_sel,
                                 const idx_t append_count) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state, std::move(column_ids));
	Append(append_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(TupleDataAppendState &append_state, DataChunk &new_chunk,
                                 const SelectionVector &append_sel, const idx_t append_count) {
	Append(append_state.pin_state, append_state.chunk_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, DataChunk &new_chunk,
                                 const SelectionVector &append_sel, const idx_t append_count) {
	TupleDataCollection::ToUnifiedFormat(chunk_state, new_chunk);
	AppendUnified(pin_state, chunk_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::AppendUnified(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                        DataChunk &new_chunk, const SelectionVector &append_sel,
                                        const idx_t append_count) {
	const idx_t actual_append_count = append_count == DConstants::INVALID_INDEX ? new_chunk.size() : append_count;
	if (actual_append_count == 0) {
		return;
	}

	if (!layout.AllConstant()) {
		TupleDataCollection::ComputeHeapSizes(chunk_state, new_chunk, append_sel, actual_append_count);
	}

	Build(pin_state, chunk_state, 0, actual_append_count);
	Scatter(chunk_state, new_chunk, append_sel, actual_append_count);
}

static inline void ToUnifiedFormatInternal(TupleDataVectorFormat &format, Vector &vector, const idx_t count) {
	vector.ToUnifiedFormat(count, format.unified);
	format.original_sel = format.unified.sel;
	format.original_owned_sel.Initialize(format.unified.owned_sel);
	switch (vector.GetType().InternalType()) {
	case PhysicalType::STRUCT: {
		auto &entries = StructVector::GetEntries(vector);
		D_ASSERT(format.children.size() == entries.size());
		for (idx_t struct_col_idx = 0; struct_col_idx < entries.size(); struct_col_idx++) {
			ToUnifiedFormatInternal(reinterpret_cast<TupleDataVectorFormat &>(format.children[struct_col_idx]),
			                        *entries[struct_col_idx], count);
		}
		break;
	}
	case PhysicalType::LIST:
		D_ASSERT(format.children.size() == 1);
		ToUnifiedFormatInternal(reinterpret_cast<TupleDataVectorFormat &>(format.children[0]),
		                        ListVector::GetEntry(vector), ListVector::GetListSize(vector));
		break;
	case PhysicalType::ARRAY: {
		D_ASSERT(format.children.size() == 1);

		// For arrays, we cheat a bit and pretend that they are lists by creating and assigning list_entry_t's to the
		// vector This allows us to reuse all the list serialization functions for array types too.
		auto array_size = ArrayType::GetSize(vector.GetType());

		// How many list_entry_t's do we need to cover the whole child array?
		// Make sure we round up so its all covered
		auto child_array_total_size = ArrayVector::GetTotalSize(vector);
		auto list_entry_t_count = MaxValue((child_array_total_size + array_size) / array_size, count);

		// Create list entries!
		format.array_list_entries = make_uniq_array<list_entry_t>(list_entry_t_count);
		for (idx_t i = 0; i < list_entry_t_count; i++) {
			format.array_list_entries[i].length = array_size;
			format.array_list_entries[i].offset = i * array_size;
		}
		format.unified.data = reinterpret_cast<data_ptr_t>(format.array_list_entries.get());

		ToUnifiedFormatInternal(reinterpret_cast<TupleDataVectorFormat &>(format.children[0]),
		                        ArrayVector::GetEntry(vector), count * array_size);
	} break;
	default:
		break;
	}
}

void TupleDataCollection::ToUnifiedFormat(TupleDataChunkState &chunk_state, DataChunk &new_chunk) {
	D_ASSERT(chunk_state.vector_data.size() >= chunk_state.column_ids.size()); // Needs InitializeAppend
	for (const auto &col_idx : chunk_state.column_ids) {
		ToUnifiedFormatInternal(chunk_state.vector_data[col_idx], new_chunk.data[col_idx], new_chunk.size());
	}
}

void TupleDataCollection::GetVectorData(const TupleDataChunkState &chunk_state, UnifiedVectorFormat result[]) {
	const auto &vector_data = chunk_state.vector_data;
	for (idx_t i = 0; i < vector_data.size(); i++) {
		const auto &source = vector_data[i].unified;
		auto &target = result[i];
		target.sel = source.sel;
		target.data = source.data;
		target.validity = source.validity;
	}
}

void TupleDataCollection::Build(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                const idx_t append_offset, const idx_t append_count) {
	auto &segment = segments.back();
	const auto size_before = segment.SizeInBytes();
	segment.allocator->Build(segment, pin_state, chunk_state, append_offset, append_count);
	data_size += segment.SizeInBytes() - size_before;
	count += append_count;
	Verify();
}

// LCOV_EXCL_START
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
// LCOV_EXCL_STOP

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
	this->segments.reserve(this->segments.size() + other.segments.size());
	for (auto &other_seg : other.segments) {
		AddSegment(std::move(other_seg));
	}
	other.Reset();
}

void TupleDataCollection::AddSegment(TupleDataSegment &&segment) {
	count += segment.count;
	data_size += segment.data_size;
	segments.emplace_back(std::move(segment));
	Verify();
}

void TupleDataCollection::Combine(unique_ptr<TupleDataCollection> other) {
	Combine(*other);
}

void TupleDataCollection::Reset() {
	count = 0;
	data_size = 0;
	segments.clear();

	// Refreshes the TupleDataAllocator to prevent holding on to allocated data unnecessarily
	allocator = make_shared_ptr<TupleDataAllocator>(*allocator);
}

void TupleDataCollection::InitializeChunk(DataChunk &chunk) const {
	chunk.Initialize(allocator->GetAllocator(), layout.GetTypes());
}

void TupleDataCollection::InitializeChunk(DataChunk &chunk, const vector<column_t> &columns) const {
	vector<LogicalType> chunk_types(columns.size());
	// keep the order of the columns
	for (idx_t i = 0; i < columns.size(); i++) {
		auto column_idx = columns[i];
		D_ASSERT(column_idx < layout.ColumnCount());
		chunk_types[i] = layout.GetTypes()[column_idx];
	}
	chunk.Initialize(allocator->GetAllocator(), chunk_types);
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

void TupleDataCollection::InitializeScan(TupleDataScanState &state, TupleDataPinProperties properties) const {
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		column_ids.push_back(i);
	}
	InitializeScan(state, std::move(column_ids), properties);
}

void TupleDataCollection::InitializeScan(TupleDataScanState &state, vector<column_t> column_ids,
                                         TupleDataPinProperties properties) const {
	state.pin_state.row_handles.clear();
	state.pin_state.heap_handles.clear();
	state.pin_state.properties = properties;
	state.segment_index = 0;
	state.chunk_index = 0;

	auto &chunk_state = state.chunk_state;

	for (auto &col : column_ids) {
		auto &type = layout.GetTypes()[col];

		if (TypeVisitor::Contains(type, LogicalTypeId::ARRAY)) {
			auto cast_type = ArrayType::ConvertToList(type);
			chunk_state.cached_cast_vector_cache.push_back(
			    make_uniq<VectorCache>(Allocator::DefaultAllocator(), cast_type));
			chunk_state.cached_cast_vectors.push_back(make_uniq<Vector>(*chunk_state.cached_cast_vector_cache.back()));
		} else {
			chunk_state.cached_cast_vectors.emplace_back();
			chunk_state.cached_cast_vector_cache.emplace_back();
		}
	}

	state.chunk_state.column_ids = std::move(column_ids);
}

void TupleDataCollection::InitializeScan(TupleDataParallelScanState &gstate, TupleDataPinProperties properties) const {
	InitializeScan(gstate.scan_state, properties);
}

void TupleDataCollection::InitializeScan(TupleDataParallelScanState &state, vector<column_t> column_ids,
                                         TupleDataPinProperties properties) const {
	InitializeScan(state.scan_state, std::move(column_ids), properties);
}

bool TupleDataCollection::Scan(TupleDataScanState &state, DataChunk &result) {
	const auto segment_index_before = state.segment_index;
	idx_t segment_index;
	idx_t chunk_index;
	if (!NextScanIndex(state, segment_index, chunk_index)) {
		if (!segments.empty()) {
			FinalizePinState(state.pin_state, segments[segment_index_before]);
		}
		result.SetCardinality(0);
		return false;
	}
	if (segment_index_before != DConstants::INVALID_INDEX && segment_index != segment_index_before) {
		FinalizePinState(state.pin_state, segments[segment_index_before]);
	}
	ScanAtIndex(state.pin_state, state.chunk_state, state.chunk_state.column_ids, segment_index, chunk_index, result);
	return true;
}

bool TupleDataCollection::Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result) {
	lstate.pin_state.properties = gstate.scan_state.pin_state.properties;

	const auto segment_index_before = lstate.segment_index;
	{
		lock_guard<mutex> guard(gstate.lock);
		if (!NextScanIndex(gstate.scan_state, lstate.segment_index, lstate.chunk_index)) {
			if (!segments.empty()) {
				FinalizePinState(lstate.pin_state, segments[segment_index_before]);
			}
			result.SetCardinality(0);
			return false;
		}
	}
	if (segment_index_before != DConstants::INVALID_INDEX && segment_index_before != lstate.segment_index) {
		FinalizePinState(lstate.pin_state, segments[lstate.segment_index]);
	}
	ScanAtIndex(lstate.pin_state, lstate.chunk_state, gstate.scan_state.chunk_state.column_ids, lstate.segment_index,
	            lstate.chunk_index, result);
	return true;
}

bool TupleDataCollection::ScanComplete(const TupleDataScanState &state) const {
	if (Count() == 0) {
		return true;
	}
	return state.segment_index == segments.size() - 1 && state.chunk_index == segments.back().ChunkCount();
}

void TupleDataCollection::FinalizePinState(TupleDataPinState &pin_state, TupleDataSegment &segment) {
	segment.allocator->ReleaseOrStoreHandles(pin_state, segment);
}

void TupleDataCollection::FinalizePinState(TupleDataPinState &pin_state) {
	D_ASSERT(!segments.empty());
	FinalizePinState(pin_state, segments.back());
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

	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (chunk_state.cached_cast_vectors[i]) {
			chunk_state.cached_cast_vectors[i]->ResetFromCache(*chunk_state.cached_cast_vector_cache[i]);
		}
	}
	Gather(chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(), chunk.count, column_ids, result,
	       *FlatVector::IncrementalSelectionVector(), chunk_state.cached_cast_vectors);
	result.SetCardinality(chunk.count);
}

// LCOV_EXCL_START
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
	idx_t total_count = 0;
	idx_t total_size = 0;
	for (const auto &segment : segments) {
		segment.Verify();
		total_count += segment.count;
		total_size += segment.data_size;
	}
	D_ASSERT(total_count == this->count);
	D_ASSERT(total_size == this->data_size);
#endif
}

void TupleDataCollection::VerifyEverythingPinned() const {
#ifdef DEBUG
	for (const auto &segment : segments) {
		segment.VerifyEverythingPinned();
	}
#endif
}
// LCOV_EXCL_STOP

} // namespace duckdb
