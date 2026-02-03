#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Geometry Uncompressed
//===--------------------------------------------------------------------===//

namespace {

//! Dictionary header size at the beginning of the string segment (offset + length)
static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
//! Marker used in length field to indicate the presence of a big string
static constexpr uint16_t BIG_GEOMETRY_MARKER = (uint16_t)-1;
//! Base size of big string marker (block id + offset)
static constexpr idx_t BIG_GEOMETRY_MARKER_BASE_SIZE = sizeof(block_id_t) + sizeof(int32_t);
//! The marker size of the big string. Padd with an additional uint32_t to make sure
// geometries are always aligned to 8 bytes.
static constexpr idx_t BIG_GEOMETRY_MARKER_SIZE = BIG_GEOMETRY_MARKER_BASE_SIZE + sizeof(uint32_t);

static constexpr idx_t DEFAULT_GEOMETRY_BLOCK_LIMIT = 4096;

//! Required alignment for geometry data in the dictionary
static constexpr idx_t GEOMETRY_ALIGNMENT = 8;

idx_t GetGeometryBlockLimit(const idx_t block_size) {
	return MinValue(AlignValueFloor(block_size / 4), DEFAULT_GEOMETRY_BLOCK_LIMIT);
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//

struct Reader {
	data_ptr_t beg;
	data_ptr_t end;
	data_ptr_t ptr;

	template <class T>
	T Read() {
#ifdef DEBUG
		if (ptr + sizeof(T) > end) {
			throw InternalException("Attempting to read past end of buffer in geometry storage");
		}
#endif
		T value = Load<T>(ptr);
		ptr += sizeof(T);
		return value;
	}

	template <class T>
	void Skip() {
		ptr += sizeof(T);
	}

	void Skip(size_t size) {
		ptr += size;
	}
};

struct Writer {
	data_ptr_t beg;
	data_ptr_t end;
	data_ptr_t ptr;

	template <class T>
	void Write(const T &value) {
#ifdef DEBUG
		if (ptr + sizeof(T) > end) {
			throw InternalException("Attempting to write past end of buffer in geometry storage");
		}
#endif
		memcpy(ptr, &value, sizeof(T));
		ptr += sizeof(T);
	}

	void Copy(const data_ptr_t source, size_t size) {
#ifdef DEBUG
		if (ptr + size > end) {
			throw InternalException("Attempting to write past end of buffer in geometry storage");
		}
#endif
		memcpy(ptr, source, size);
		ptr += size;
	}
};

geometry_t DeserializeGeometry(Reader &reader, ArenaAllocator &arena) {
	const auto geom_type = static_cast<GeometryType>(reader.Read<uint8_t>());
	const auto vert_type = static_cast<VertexType>(reader.Read<uint8_t>());

	reader.Skip<uint8_t>(); // Padding
	reader.Skip<uint8_t>(); // Padding

	const auto item_count = reader.Read<uint32_t>();

	geometry_t geom(geom_type, vert_type, nullptr, item_count);

	if (item_count == 0) {
		return geom;
	}

	switch (geom_type) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vert_count = item_count;
		const auto vert_width = geom.GetWidth();

		if (PointerIsAligned<double>(reader.ptr)) {
			const auto vert_array = reinterpret_cast<double *>(reader.ptr);
			geom.SetVerts(vert_array, vert_count);
			reader.Skip(vert_count * vert_width);
			return geom;
		}

		// Otherwise we need to copy
		const auto item_array = arena.AllocateAligned(vert_count * vert_width);
		const auto vert_array = reinterpret_cast<double *>(item_array);

		memcpy(vert_array, reader.ptr, vert_count * vert_width);
		geom.SetVerts(vert_array, vert_count);
		return geom;
	}
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = item_count;

		const auto item_array = arena.AllocateAligned(part_count * sizeof(geometry_t));
		const auto part_array = reinterpret_cast<geometry_t *>(item_array);

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			part_array[part_idx] = DeserializeGeometry(reader, arena);
		}

		geom.SetParts(part_array, part_count);
		return geom;
	}
	default:
		throw InternalException("Unknown geometry type %d in compression", static_cast<int>(geom_type));
	}
}

geometry_t DeserializeGeometry(data_ptr_t data, uint32_t length, Vector &result) {
	Reader reader;

	reader.beg = data;
	reader.end = data + length;
	reader.ptr = data;

	return DeserializeGeometry(reader, GeometryVector::GetArena(result));
}

void SerializeGeometry(Writer &writer, const geometry_t &geom) {
	const auto geom_type = geom.GetType();
	const auto vert_type = geom.GetVertType();
	const auto item_count = geom.GetCount();

	writer.Write(static_cast<uint8_t>(geom_type));
	writer.Write(static_cast<uint8_t>(vert_type));
	writer.Write<uint8_t>(0); // Padding
	writer.Write<uint8_t>(0); // Padding
	writer.Write<uint32_t>(item_count);

	switch (geom_type) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vert_count = item_count;
		const auto vert_width = geom.GetWidth();
		const auto vert_array = geom.GetDataPointer();

		writer.Copy(vert_array, vert_count * vert_width);

	} break;
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = item_count;
		const auto part_array = geom.GetParts();

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			SerializeGeometry(writer, part_array[part_idx]);
		}
	} break;
	default:
		throw InternalException("Unknown geometry type %d in compression", static_cast<int>(geom_type));
	}
}

void SerializeGeometry(const geometry_t &geom, data_ptr_t ptr, uint32_t len) {
	Writer writer;
	writer.beg = ptr;
	writer.end = ptr + len;
	writer.ptr = ptr;

	SerializeGeometry(writer, geom);
}

idx_t GetSerializedSize(const geometry_t &geom) {
	switch (geom.GetType()) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vert_count = geom.GetCount();
		const auto vert_width = geom.GetWidth();
		return 8 + (vert_count * vert_width);
	}
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();

		idx_t total_size = 8;
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			total_size += GetSerializedSize(part_array[part_idx]);
		}
		return total_size;
	}
	default:
		throw InternalException("Unknown geometry type %d in compression", static_cast<int>(geom.GetType()));
	}
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

using UncompressedGeometrySegmentState = UncompressedStringSegmentState;

struct GeometryDictionary {
	//! The size of the dictionary
	uint32_t size;
	//! The end of the dictionary, which defaults to the block size.
	uint32_t end;

	void Verify(const idx_t block_size) {
		D_ASSERT(size <= block_size);
		D_ASSERT(end <= block_size);
		D_ASSERT(size <= end);
	}
};

void SetDictionary(ColumnSegment &segment, BufferHandle &handle, GeometryDictionary container) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	Store<uint32_t>(container.size, startptr);
	Store<uint32_t>(container.end, startptr + sizeof(uint32_t));
}

GeometryDictionary GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	GeometryDictionary container;
	container.size = Load<uint32_t>(startptr);
	container.end = Load<uint32_t>(startptr + sizeof(uint32_t));
	return container;
}

uint32_t GetDictionaryEnd(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	return Load<uint32_t>(startptr + sizeof(uint32_t));
}

idx_t RemainingSpace(ColumnSegment &segment, BufferHandle &handle) {
	auto dictionary = GetDictionary(segment, handle);
	D_ASSERT(dictionary.end == segment.SegmentSize());
	idx_t used_space = dictionary.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
	D_ASSERT(segment.SegmentSize() >= used_space);
	return segment.SegmentSize() - used_space;
}

//! Align size up to 8 bytes for geometry storage
static inline idx_t AlignGeometrySize(idx_t size) {
	return AlignValue<idx_t, GEOMETRY_ALIGNMENT>(size);
}

geometry_t ReadGeometry(data_ptr_t target, int32_t offset, uint32_t length, Vector &result) {
	return DeserializeGeometry(target + offset, length, result);
}

geometry_t ReadGeometryWithLength(data_ptr_t target, int32_t offset, Vector &result) {
	const auto ptr = target + offset;
	const auto size = Load<uint32_t>(ptr);
	const auto data = ptr + sizeof(uint32_t);
	return DeserializeGeometry(data, size, result);
}

void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

void ReadGeometryMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset) {
	memcpy(&block_id, target, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(&offset, target, sizeof(int32_t));
}

void WriteGeometryMemory(ColumnSegment &segment, const geometry_t &geom, block_id_t &result_block,
                         int32_t &result_offset) {
	// TODO: pass this down, dont recompute here
	const auto serialized_size = GetSerializedSize(geom);
	const auto total_length = UnsafeNumericCast<uint32_t>(serialized_size + sizeof(uint32_t));

	shared_ptr<BlockHandle> block;
	BufferHandle handle;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto &state = segment.GetSegmentState()->Cast<UncompressedGeometrySegmentState>();
	// check if the string fits in the current block
	if (!state.head || state.head->offset + total_length >= state.head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		auto alloc_size = MaxValue<idx_t>(total_length, segment.GetBlockSize());
		auto new_block = make_uniq<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, alloc_size, false);
		block = handle.GetBlockHandle();
		state.overflow_blocks.insert(make_pair(block->BlockId(), reference<StringBlock>(*new_block)));
		new_block->block = std::move(block);
		new_block->next = std::move(state.head);
		state.head = std::move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = buffer_manager.Pin(state.head->block);
	}

	result_block = state.head->block->BlockId();
	result_offset = UnsafeNumericCast<int32_t>(state.head->offset);

	// copy the string and the length there
	auto ptr = handle.Ptr() + state.head->offset;
	Store<uint32_t>(UnsafeNumericCast<uint32_t>(serialized_size), ptr);
	ptr += sizeof(uint32_t);

	// serialize the data into the buffer
	SerializeGeometry(geom, ptr, serialized_size);

	state.head->offset += total_length;
}

void WriteGeometry(ColumnSegment &segment, const geometry_t &geom, block_id_t &result_block, int32_t &result_offset) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedGeometrySegmentState>();
	if (state.overflow_writer) {
		// overflow writer is set: write string there
		state.overflow_writer->WriteGeometry(state, geom, result_block, result_offset);
	} else {
		// default overflow behavior: use in-memory buffer to store the overflow string
		WriteGeometryMemory(segment, geom, result_block, result_offset);
	}
}

geometry_t ReadOverflowGeometry(ColumnSegment &segment, Vector &result, block_id_t block, int32_t offset) {
	auto &buffer_manager = segment.block->GetBufferManager();
	auto &state = segment.GetSegmentState()->Cast<UncompressedGeometrySegmentState>();

	D_ASSERT(block != INVALID_BLOCK);
	D_ASSERT(offset < NumericCast<int32_t>(segment.GetBlockSize()));

	if (block >= MAXIMUM_BLOCK) {
		// read the overflow geometry from memory
		// first pin the handle, if it is not pinned yet
		const auto entry = state.overflow_blocks.find(block);
		D_ASSERT(entry != state.overflow_blocks.end());
		auto handle = buffer_manager.Pin(entry->second.get().block);
		auto final_buffer = handle.Ptr();
		GeometryVector::AddHandle(result, std::move(handle));
		return ReadGeometryWithLength(final_buffer, offset, result);
	}

	// read the overflow geometry from disk
	// pin the initial handle and read the length
	auto block_handle = state.GetHandle(segment.block->GetBlockManager(), block);
	auto handle = buffer_manager.Pin(block_handle);

	// read header
	auto length = Load<uint32_t>(handle.Ptr() + offset);
	auto remaining = length;
	offset += sizeof(uint32_t);

	// Does this overflow geometry fit within a single block?
	const bool allocate_block = length >= segment.GetBlockSize();
	if (allocate_block) {
		// overflow geometry is bigger than a block - allocate a temporary buffer for it
		auto target_handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, length);
		auto target_ptr = target_handle.Ptr();

		// Start scooping up the geometry
		while (remaining > 0) {
			idx_t to_write = MinValue<idx_t>(remaining, segment.GetBlockSize() - sizeof(block_id_t) -
			                                                UnsafeNumericCast<idx_t>(offset));
			memcpy(target_ptr, handle.Ptr() + offset, to_write);
			remaining -= to_write;
			offset += UnsafeNumericCast<int32_t>(to_write);
			target_ptr += to_write;
			if (remaining > 0) {
				// read the next block
				block_id_t next_block = Load<block_id_t>(handle.Ptr() + offset);
				block_handle = state.GetHandle(segment.block->GetBlockManager(), next_block);
				handle = buffer_manager.Pin(block_handle);
				offset = 0;
			}
		}

		auto final_buffer = target_handle.Ptr();
		GeometryVector::AddHandle(result, std::move(target_handle));
		return ReadGeometry(final_buffer, 0, length, result);

	} else {
		// overflow is smaller than a block - add it to the vector directly
		auto target_ptr = GeometryVector::GetArena(result).AllocateAligned(length);
		auto source_ptr = target_ptr;

		// Start scooping up the geometry
		while (remaining > 0) {
			idx_t to_write = MinValue<idx_t>(remaining, segment.GetBlockSize() - sizeof(block_id_t) -
			                                                UnsafeNumericCast<idx_t>(offset));

			memcpy(target_ptr, handle.Ptr() + offset, to_write);
			remaining -= to_write;
			offset += UnsafeNumericCast<int32_t>(to_write);
			target_ptr += to_write;
			if (remaining > 0) {
				// read the next block
				block_id_t next_block = Load<block_id_t>(handle.Ptr() + offset);
				block_handle = state.GetHandle(segment.block->GetBlockManager(), next_block);
				handle = buffer_manager.Pin(block_handle);
				offset = 0;
			}
		}

		// TODO: We're copying the parts twice here, in the future we should be able to deserialize as we parse
		// the blocks in one go.
		return ReadGeometry(source_ptr, 0, length, result);
	}
}

geometry_t FetchGeometryFromDict(ColumnSegment &segment, uint32_t dict_end_offset, Vector &result, data_ptr_t base_ptr,
                                 int32_t dict_offset, uint32_t blob_length) {
	D_ASSERT(dict_offset <= NumericCast<int32_t>(segment.GetBlockSize()));

	if (DUCKDB_LIKELY(dict_offset >= 0)) {
		const auto dict_end = base_ptr + dict_end_offset;
		const auto dict_pos = dict_end - dict_offset;

		// This is a regular geometry, just read it from the dictionary, and zero-copy the vertex data if possible
		return ReadGeometry(dict_pos, 0, blob_length, result);
	}

	// This is an overflow geometry, read the block id and offset marker from the dictionary,
	// and then read the actual geometry from there

	block_id_t block_id;
	int32_t offset;
	ReadGeometryMarker(base_ptr + dict_end_offset - AbsValue<int32_t>(dict_offset), block_id, offset);

	return ReadOverflowGeometry(segment, result, block_id, offset);
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//

struct GeometryAnalyzeState : public AnalyzeState {
	explicit GeometryAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {
	}

	idx_t count = 0;
	idx_t total_string_size = 0;
	idx_t overflow_strings = 0;
};

unique_ptr<AnalyzeState> GeometryInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager());
	return make_uniq<GeometryAnalyzeState>(info);
}

bool GeometryAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<GeometryAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	state.count += count;
	auto data = UnifiedVectorFormat::GetData<geometry_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			// Get total serialized size
			auto string_size = GetSerializedSize(data[idx]);

			state.total_string_size += string_size;
			if (string_size >= GetGeometryBlockLimit(state.info.GetBlockSize())) {
				state.overflow_strings++;
			}
		}
	}

	return true;
}

idx_t GeometryFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<GeometryAnalyzeState>();
	// Pessimistic estimate: each small geometry may need up to (GEOMETRY_ALIGNMENT - 1) bytes of padding
	idx_t small_geometry_count = state.count - state.overflow_strings;
	idx_t alignment_padding = small_geometry_count * (GEOMETRY_ALIGNMENT - 1);
	return state.count * sizeof(int32_t) + state.total_string_size + alignment_padding +
	       state.overflow_strings * BIG_GEOMETRY_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct GeometryScanState final : public SegmentScanState {
	BufferHandle handle;
};

void GeometryInitPrefetch(ColumnSegment &segment, PrefetchState &prefetch_state) {
	prefetch_state.AddBlock(segment.block);
	auto segment_state = segment.GetSegmentState();
	if (segment_state) {
		auto &state = segment_state->Cast<UncompressedGeometrySegmentState>();
		auto &block_manager = segment.block->GetBlockManager();
		for (auto &block_id : state.on_disk_blocks) {
			auto block_handle = state.GetHandle(block_manager, block_id);
			prefetch_state.AddBlock(block_handle);
		}
	}
}

unique_ptr<SegmentScanState> GeometryInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<GeometryScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void GeometryScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<GeometryScanState>();
	auto start = state.GetPositionInSegment();

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict_end = GetDictionaryEnd(segment, scan_state.handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<geometry_t>(result);

	int32_t previous_offset = start > 0 ? base_data[start - 1] : 0;

	for (idx_t i = 0; i < scan_count; i++) {
		// std::abs used since offsets can be negative to indicate big strings
		auto current_offset = base_data[start + i];
		auto string_length = UnsafeNumericCast<uint32_t>(std::abs(current_offset) - std::abs(previous_offset));
		result_data[result_offset + i] =
		    FetchGeometryFromDict(segment, dict_end, result, baseptr, current_offset, string_length);
		previous_offset = base_data[start + i];
	}
}

void GeometryScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	GeometryScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
void GeometrySelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                    const SelectionVector &sel, idx_t sel_count) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<GeometryScanState>();
	auto start = state.GetPositionInSegment();

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict_end = GetDictionaryEnd(segment, scan_state.handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<geometry_t>(result);

	for (idx_t i = 0; i < sel_count; i++) {
		idx_t index = start + sel.get_index(i);
		auto current_offset = base_data[index];
		auto prev_offset = index > 0 ? base_data[index - 1] : 0;
		auto string_length = UnsafeNumericCast<uint32_t>(std::abs(current_offset) - std::abs(prev_offset));
		result_data[i] = FetchGeometryFromDict(segment, dict_end, result, baseptr, current_offset, string_length);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void GeometryFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto &handle = state.GetOrInsertHandle(segment);

	auto baseptr = handle.Ptr() + segment.GetBlockOffset();
	auto dict_end = GetDictionaryEnd(segment, handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<geometry_t>(result);

	auto dict_offset = base_data[row_id];
	uint32_t string_length;
	if (DUCKDB_UNLIKELY(row_id == 0LL)) {
		// edge case where this is the first string in the dict
		string_length = NumericCast<uint32_t>(std::abs(dict_offset));
	} else {
		string_length = NumericCast<uint32_t>(std::abs(dict_offset) - std::abs(base_data[row_id - 1]));
	}
	result_data[result_idx] = FetchGeometryFromDict(segment, dict_end, result, baseptr, dict_offset, string_length);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//

unique_ptr<CompressedSegmentState> GeometryInitSegment(ColumnSegment &segment, block_id_t block_id,
                                                       optional_ptr<ColumnSegmentState> segment_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.block);
		GeometryDictionary dictionary;
		dictionary.size = 0;
		dictionary.end = UnsafeNumericCast<uint32_t>(segment.SegmentSize());
		SetDictionary(segment, handle, dictionary);
	}
	auto result = make_uniq<UncompressedGeometrySegmentState>();
	if (segment_state) {
		auto &serialized_state = segment_state->Cast<SerializedStringSegmentState>();
		result->on_disk_blocks = std::move(serialized_state.blocks);
	}
	return std::move(result);
}

unique_ptr<CompressionAppendState> GeometryInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	// This block was initialized in StringInitSegment
	auto handle = buffer_manager.Pin(segment.block);
	return make_uniq<CompressionAppendState>(std::move(handle));
}

idx_t GeometryAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                     UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	auto &handle = append_state.handle;

	D_ASSERT(segment.GetBlockOffset() == 0);
	auto handle_ptr = handle.Ptr();
	auto source_data = UnifiedVectorFormat::GetData<geometry_t>(data);
	auto result_data = reinterpret_cast<int32_t *>(handle_ptr + DICTIONARY_HEADER_SIZE);
	auto dictionary_size = reinterpret_cast<uint32_t *>(handle_ptr);
	auto dictionary_end = reinterpret_cast<uint32_t *>(handle_ptr + sizeof(uint32_t));

	idx_t remaining_space = RemainingSpace(segment, handle);
	auto base_count = segment.count.load();
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = data.sel->get_index(offset + i);
		auto target_idx = base_count + i;
		if (remaining_space < sizeof(int32_t)) {
			// string index does not fit in the block at all
			segment.count += i;
			return i;
		}
		remaining_space -= sizeof(int32_t);
		const bool is_null = !data.validity.RowIsValid(source_idx);
		if (is_null) {
			stats.statistics.SetHasNullFast();
			// null value is stored as a copy of the last value, this is done to be able to efficiently do the
			// string_length calculation
			if (target_idx > 0) {
				result_data[target_idx] = result_data[target_idx - 1];
			} else {
				result_data[target_idx] = 0;
			}
			continue;
		}
		auto end = handle.Ptr() + *dictionary_end;

#ifdef DEBUG
		GetDictionary(segment, handle).Verify(segment.GetBlockSize());
#endif
		// Unknown geometry, continue
		// non-null value, check if we can fit it within the block

		const auto serialized_size = GetSerializedSize(source_data[source_idx]);

		// determine whether or not we have space in the block for this geometry
		bool use_overflow_block = false;

		auto required_space = serialized_size;
		if (DUCKDB_UNLIKELY(required_space >= GetGeometryBlockLimit(segment.GetBlockSize()))) {
			// string exceeds block limit, store in overflow block and only write a marker here
			required_space = BIG_GEOMETRY_MARKER_SIZE; // Already 16 bytes (8-byte aligned)
			use_overflow_block = true;
		} else {
			// Align small geometry space to 8 bytes to ensure subsequent geometries
			// are written at 8-byte aligned positions (required for geometry_t arrays)
			required_space = AlignGeometrySize(required_space);
		}
		if (DUCKDB_UNLIKELY(required_space > remaining_space)) {
			// no space remaining: return how many tuples we ended up writing
			segment.count += i;
			return i;
		}

		// we have space: write the string
		stats.statistics.SetHasNoNullFast();
		GeometryStats::Update(stats.statistics, source_data[source_idx]);

		if (DUCKDB_UNLIKELY(use_overflow_block)) {
			// write to overflow blocks
			block_id_t block;
			int32_t current_offset;
			// write the geometry into the current string block
			WriteGeometry(segment, source_data[source_idx], block, current_offset);
			*dictionary_size += BIG_GEOMETRY_MARKER_SIZE;
			remaining_space -= BIG_GEOMETRY_MARKER_SIZE;
			auto dict_pos = end - *dictionary_size;

			// write a big geometry marker into the dictionary
			WriteStringMarker(dict_pos, block, current_offset);

			// place the dictionary offset into the set of vectors
			// note: for overflow strings we write negative value

			// dictionary_size is an uint32_t value, so we can cast up.
			D_ASSERT(NumericCast<idx_t>(*dictionary_size) <= segment.GetBlockSize());
			result_data[target_idx] = -NumericCast<int32_t>((*dictionary_size));
		} else {
			// string fits in block, append to dictionary and increment dictionary position
			D_ASSERT(serialized_size < NumericLimits<uint16_t>::Maximum());

			// required_space is already aligned from above
			*dictionary_size += required_space;
			remaining_space -= required_space;
			auto dict_pos = end - *dictionary_size;

			// Verify alignment (dict_pos must be 8-byte aligned for geometry_t arrays)
			D_ASSERT(reinterpret_cast<uintptr_t>(dict_pos) % GEOMETRY_ALIGNMENT == 0);

			// now write the actual geometry data into the dictionary
			SerializeGeometry(source_data[source_idx], dict_pos, UnsafeNumericCast<uint32_t>(serialized_size));

			// dictionary_size is an uint32_t value, so we can cast up.
			D_ASSERT(NumericCast<idx_t>(*dictionary_size) <= segment.GetBlockSize());
			// Place the dictionary offset into the set of vectors.
			result_data[target_idx] = NumericCast<int32_t>(*dictionary_size);
		}
		D_ASSERT(RemainingSpace(segment, handle) <= segment.GetBlockSize());
#ifdef DEBUG
		GetDictionary(segment, handle).Verify(segment.GetBlockSize());
#endif
	}
	segment.count += count;
	return count;
}

idx_t FinalizeAppend(ColumnSegment &segment, SegmentStatistics &) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dict = GetDictionary(segment, handle);
	D_ASSERT(dict.end == segment.SegmentSize());

	// compute the total size required to store this segment
	auto offset_size = DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
	// Align the boundary between offset array and dictionary to maintain 8-byte alignment
	auto aligned_offset_size = AlignGeometrySize(offset_size);
	auto total_size = aligned_offset_size + dict.size;

	CompressionInfo info(segment.block->GetBlockManager());
	if (total_size >= info.GetCompactionFlushLimit()) {
		// the block is full enough, don't bother moving around the dictionary
		return segment.SegmentSize();
	}

	// the block has space left: figure out how much space we can save
	// move the dictionary so it lines up with the aligned offset boundary
	auto dataptr = handle.Ptr();
	memmove(dataptr + aligned_offset_size, dataptr + dict.end - dict.size, dict.size);
	dict.end = UnsafeNumericCast<uint32_t>(total_size);

	// write the new dictionary (with the updated "end")
	SetDictionary(segment, handle, dict);
	return total_size;
}

} // namespace

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction GeometryUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::GEOMETRY);
	return CompressionFunction(
	    CompressionType::COMPRESSION_UNCOMPRESSED, data_type, GeometryInitAnalyze, GeometryAnalyze,
	    GeometryFinalAnalyze, UncompressedFunctions::InitCompression, UncompressedFunctions::Compress,
	    UncompressedFunctions::FinalizeCompress, GeometryInitScan, GeometryScan, GeometryScanPartial, GeometryFetchRow,
	    UncompressedFunctions::EmptySkip, GeometryInitSegment, GeometryInitAppend, GeometryAppend, FinalizeAppend,
	    nullptr, UncompressedStringStorage::SerializeState, UncompressedStringStorage::DeserializeState,
	    UncompressedStringStorage::VisitBlockIds, GeometryInitPrefetch, GeometrySelect);
}

} // namespace duckdb
