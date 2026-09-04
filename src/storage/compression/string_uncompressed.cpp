#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/storage/string_uncompressed.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

[[noreturn]] static void ThrowInvalidStringDictionary() {
	throw DataCorruptionException("Corrupted uncompressed string segment: dictionary is outside the segment");
}

[[noreturn]] static void ThrowStringOffsetTableOutOfBounds() {
	throw DataCorruptionException("Corrupted uncompressed string segment: offset table overlaps the dictionary");
}

[[noreturn]] static void ThrowStringOffsetMinimumValue() {
	throw DataCorruptionException("Corrupted uncompressed string segment: dictionary offset is INT32_MIN");
}

[[noreturn]] static void ThrowStringOffsetOutsideDictionary(uint32_t offset, uint32_t dictionary_size) {
	throw DataCorruptionException(
	    "Corrupted uncompressed string segment: dictionary offset %u exceeds dictionary size %u", offset,
	    dictionary_size);
}

[[noreturn]] static void ThrowDecreasingStringOffset(uint32_t offset, uint32_t previous_offset) {
	throw DataCorruptionException(
	    "Corrupted uncompressed string segment: dictionary offset %u is smaller than preceding offset %u", offset,
	    previous_offset);
}

[[noreturn]] static void ThrowInvalidOverflowStringMarker() {
	throw DataCorruptionException(
	    "Corrupted uncompressed string segment: negative dictionary offset does not describe an overflow marker");
}

[[noreturn]] static void ThrowInvalidOverflowStringBlock() {
	throw DataCorruptionException("Corrupted uncompressed string segment: invalid overflow string block ID");
}

[[noreturn]] static void ThrowOverflowStringOffsetOutOfBounds() {
	throw DataCorruptionException("Corrupted uncompressed string segment: overflow string offset is outside its block");
}

uint32_t StringSegmentLayout::ValidateAndGetDictionaryOffset(int32_t encoded_offset) const {
	if (encoded_offset == NumericLimits<int32_t>::Minimum()) {
		ThrowStringOffsetMinimumValue();
	}

	uint32_t dictionary_offset;
	if (encoded_offset < 0) {
		dictionary_offset = NumericCast<uint32_t>(-encoded_offset);
	} else {
		dictionary_offset = NumericCast<uint32_t>(encoded_offset);
	}
	if (dictionary_offset > dictionary.size) {
		ThrowStringOffsetOutsideDictionary(dictionary_offset, dictionary.size);
	}
	return dictionary_offset;
}

StringDictionaryEntry StringSegmentLayout::CreateDictionaryEntry(int32_t current_offset, int32_t previous_offset,
                                                                 uint32_t previous_dictionary_offset) const {
	auto current_dictionary_offset = ValidateAndGetDictionaryOffset(current_offset);
	if (current_dictionary_offset < previous_dictionary_offset) {
		ThrowDecreasingStringOffset(current_dictionary_offset, previous_dictionary_offset);
	}

	// Offsets store the cumulative number of dictionary bytes used.
	auto string_length = current_dictionary_offset - previous_dictionary_offset;

	// If the offset is negative, the entry is either NULL or an overflow string.
	if (current_offset < 0) {
		// If it is NULL, the current offset must be inherited unchanged from the previous entry.
		if (string_length == 0 && current_offset != previous_offset) {
			ThrowInvalidOverflowStringMarker();
		}

		// If it is an overflow string, the entry's length must match the marker's length.
		if (string_length > 0 && string_length != UncompressedStringStorage::BIG_STRING_MARKER_SIZE) {
			ThrowInvalidOverflowStringMarker();
		}
	}

	auto is_overflow = current_offset < 0 && string_length > 0;
	return {dictionary_data.SubArray(dictionary.size - current_dictionary_offset, string_length), is_overflow};
}

StringDictionaryEntry StringSegmentLayout::GetDictionaryEntry(idx_t row_index) const {
	D_ASSERT(row_index < offsets.size());
	auto current_offset = offsets[row_index];
	auto previous_offset = row_index > 0 ? offsets[row_index - 1] : 0;
	auto previous_dictionary_offset = ValidateAndGetDictionaryOffset(previous_offset);
	return CreateDictionaryEntry(current_offset, previous_offset, previous_dictionary_offset);
}

StringSegmentLayout StringSegmentLayout::Read(const BufferHandle &handle, const ColumnSegment &segment) {
	auto reader = CompressionSegmentReader::FromSegment(handle, segment, "uncompressed string segment");
	StringDictionaryContainer dictionary {reader.Read<uint32_t>(), reader.Read<uint32_t>()};
	if (dictionary.end > reader.Size() || dictionary.size > dictionary.end) {
		ThrowInvalidStringDictionary();
	}
	auto dictionary_start = NumericCast<idx_t>(dictionary.end - dictionary.size);
	auto offset_count = segment.count.load();
	if (dictionary_start < UncompressedStringStorage::DICTIONARY_HEADER_SIZE ||
	    offset_count > (dictionary_start - UncompressedStringStorage::DICTIONARY_HEADER_SIZE) / sizeof(int32_t)) {
		ThrowStringOffsetTableOutOfBounds();
	}
	auto dictionary_data = reader.GetBytes(dictionary_start, dictionary.size);
	auto offsets = reader.GetArray<int32_t>(UncompressedStringStorage::DICTIONARY_HEADER_SIZE, offset_count);
	return {dictionary, dictionary_data, offsets};
}

StringScanState::StringScanState(BufferHandle handle_p) : handle(std::move(handle_p)) {
}

//===--------------------------------------------------------------------===//
// Storage Class
//===--------------------------------------------------------------------===//
UncompressedStringSegmentState::~UncompressedStringSegmentState() {
	while (head) {
		// prevent deep recursion here
		head = std::move(head->next);
	}
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct StringAnalyzeState : public AnalyzeState {
	explicit StringAnalyzeState(BlockManager &block_manager)
	    : AnalyzeState(block_manager), count(0), total_string_size(0), overflow_strings(0) {
	}

	idx_t count;
	idx_t total_string_size;
	idx_t overflow_strings;
};

unique_ptr<AnalyzeState> UncompressedStringStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<StringAnalyzeState>(col_data.GetBlockManager());
}

bool UncompressedStringStorage::StringAnalyze(AnalyzeState &state_p, const Vector &input) {
	auto &state = state_p.Cast<StringAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(vdata);

	const auto count = input.size();
	state.count += count;
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			auto string_size = data[idx].GetSize();
			state.total_string_size += string_size;
			if (string_size >= StringUncompressed::GetStringBlockLimit(state.info.GetBlockSize())) {
				state.overflow_strings++;
			}
		}
	}
	return true;
}

idx_t UncompressedStringStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<StringAnalyzeState>();
	return state.count * sizeof(int32_t) + state.total_string_size + state.overflow_strings * BIG_STRING_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void UncompressedStringInitPrefetch(ColumnSegment &segment, PrefetchState &prefetch_state) {
	prefetch_state.AddBlock(segment.GetBlockHandle());
	auto segment_state = segment.GetSegmentState();
	if (segment_state) {
		auto &state = segment_state->Cast<UncompressedStringSegmentState>();
		auto &block_manager = segment.GetBlockHandle()->GetBlockManager();
		for (auto &block_id : state.on_disk_blocks) {
			auto block_handle = state.GetHandle(block_manager, block_id);
			prefetch_state.AddBlock(block_handle);
		}
	}
}

unique_ptr<SegmentScanState> UncompressedStringStorage::StringInitScan(const QueryContext &context,
                                                                       ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(context, segment.GetBlockHandle());
	return make_uniq<StringScanState>(std::move(handle));
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                  Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<StringScanState>();
	auto layout = StringSegmentLayout::Read(scan_state.handle, segment);
	auto start = state.GetPositionInSegment();
	D_ASSERT(start <= segment.count.load());
	D_ASSERT(scan_count <= segment.count.load() - start);

	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	int32_t previous_offset = 0;
	uint32_t previous_dictionary_offset = 0;
	if (start > 0) {
		previous_offset = layout.offsets[start - 1];
		previous_dictionary_offset = layout.ValidateAndGetDictionaryOffset(previous_offset);
	}

	for (idx_t i = 0; i < scan_count; i++) {
		auto current_offset = layout.offsets[start + i];
		auto entry = layout.CreateDictionaryEntry(current_offset, previous_offset, previous_dictionary_offset);
		result_data[result_offset + i] = FetchStringFromEntry(state.context, segment, result, entry);
		previous_offset = current_offset;
		previous_dictionary_offset += UnsafeNumericCast<uint32_t>(entry.data.size());
	}
}

void UncompressedStringStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                           Vector &result) {
	StringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::Select(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                       Vector &result, const SelectionVector &sel, idx_t sel_count) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<StringScanState>();
	auto layout = StringSegmentLayout::Read(scan_state.handle, segment);
	auto start = state.GetPositionInSegment();
	D_ASSERT(start <= segment.count.load());
	D_ASSERT(vector_count <= segment.count.load() - start);
	D_ASSERT(sel_count <= vector_count);

	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	for (idx_t i = 0; i < sel_count; i++) {
		auto selection_index = sel.get_index(i);
		D_ASSERT(selection_index < vector_count);
		idx_t index = start + selection_index;
		auto entry = layout.GetDictionaryEntry(index);
		result_data[i] = FetchStringFromEntry(state.context, segment, result, entry);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
BufferHandle &ColumnFetchState::GetOrInsertHandle(ColumnSegment &segment) {
	auto primary_id = segment.GetBlockHandle()->BlockId();

	auto entry = handles.find(primary_id);
	if (entry == handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
		auto handle = buffer_manager.Pin(context, segment.GetBlockHandle());
		auto pinned_entry = handles.insert(make_pair(primary_id, std::move(handle)));
		return pinned_entry.first->second;
	} else {
		// already pinned: use the pinned handle
		return entry->second;
	}
}

void UncompressedStringStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                               Vector &result, idx_t result_idx) {
	D_ASSERT(row_id >= 0);
	auto row_index = NumericCast<idx_t>(row_id);
	D_ASSERT(row_index < segment.count.load());

	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto &handle = state.GetOrInsertHandle(segment);
	auto layout = StringSegmentLayout::Read(handle, segment);

	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	auto entry = layout.GetDictionaryEntry(row_index);
	result_data[result_idx] = FetchStringFromEntry(state.context, segment, result, entry);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
SerializedStringSegmentState::SerializedStringSegmentState() {
}

SerializedStringSegmentState::SerializedStringSegmentState(vector<block_id_t> blocks_p) {
	blocks = std::move(blocks_p);
}

void SerializedStringSegmentState::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(1, "overflow_blocks", blocks);
}

unique_ptr<CompressedSegmentState>
UncompressedStringStorage::StringInitSegment(ColumnSegment &segment, block_id_t block_id,
                                             optional_ptr<ColumnSegmentState> segment_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.GetBlockHandle());
		StringDictionaryContainer dictionary;
		dictionary.size = 0;
		dictionary.end = UnsafeNumericCast<uint32_t>(segment.SegmentSize());
		SetDictionary(segment, handle, dictionary);
	}
	auto result = make_uniq<UncompressedStringSegmentState>();
	if (segment_state) {
		auto &serialized_state = segment_state->Cast<SerializedStringSegmentState>();
		result->on_disk_blocks = std::move(serialized_state.blocks);
	}
	return std::move(result);
}

idx_t UncompressedStringStorage::FinalizeAppend(ColumnSegment &segment, BaseStatistics &) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(segment.GetBlockHandle());
	auto dict = GetDictionary(segment, handle);
	D_ASSERT(dict.end == segment.SegmentSize());
	// compute the total size required to store this segment
	auto offset_size = DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
	auto total_size = offset_size + dict.size;

	CompressionInfo info(segment.GetBlockHandle()->GetBlockManager());
	if (total_size >= info.GetCompactionFlushLimit()) {
		// the block is full enough, don't bother moving around the dictionary
		return segment.SegmentSize();
	}

	// the block has space left: figure out how much space we can save
	auto move_amount = segment.SegmentSize() - total_size;
	// move the dictionary so it lines up exactly with the offsets
	auto dataptr = handle.GetDataMutable() + segment.GetBlockOffset();
	memmove(dataptr + offset_size, dataptr + dict.end - dict.size, dict.size);
	dict.end -= move_amount;
	D_ASSERT(dict.end == total_size);
	// write the new dictionary (with the updated "end")
	SetDictionary(segment, handle, dict);
	return total_size;
}

//===--------------------------------------------------------------------===//
// Serialization & Cleanup
//===--------------------------------------------------------------------===//
unique_ptr<ColumnSegmentState> UncompressedStringStorage::SerializeState(ColumnSegment &segment) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (state.on_disk_blocks.empty()) {
		// no on-disk blocks - nothing to write
		return nullptr;
	}
	return make_uniq<SerializedStringSegmentState>(state.on_disk_blocks);
}

unique_ptr<ColumnSegmentState> UncompressedStringStorage::DeserializeState(Deserializer &deserializer) {
	auto result = make_uniq<SerializedStringSegmentState>();
	deserializer.ReadProperty(1, "overflow_blocks", result->blocks);
	return std::move(result);
}

void UncompressedStringStorage::VisitBlockIds(const ColumnSegment &segment, BlockIdVisitor &visitor) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	for (auto &block_id : state.on_disk_blocks) {
		visitor.Visit(block_id);
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction StringUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type,
	                           UncompressedStringStorage::StringInitAnalyze, UncompressedStringStorage::StringAnalyze,
	                           UncompressedStringStorage::StringFinalAnalyze, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           UncompressedStringStorage::StringInitScan, UncompressedStringStorage::StringScan,
	                           UncompressedStringStorage::StringScanPartial, UncompressedStringStorage::StringFetchRow,
	                           UncompressedFunctions::EmptySkip, UncompressedStringStorage::StringInitSegment,
	                           UncompressedStringStorage::StringInitAppend, UncompressedStringStorage::StringAppend,
	                           UncompressedStringStorage::FinalizeAppend, UncompressedStringStorage::StringRevertAppend,
	                           UncompressedStringStorage::SerializeState, UncompressedStringStorage::DeserializeState,
	                           UncompressedStringStorage::VisitBlockIds, UncompressedStringInitPrefetch,
	                           UncompressedStringStorage::Select);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                              StringDictionaryContainer container) {
	auto startptr = handle.GetDataMutable() + segment.GetBlockOffset();
	Store<uint32_t>(container.size, startptr);
	Store<uint32_t>(container.end, startptr + sizeof(uint32_t));
}

StringDictionaryContainer UncompressedStringStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(startptr);
	container.end = Load<uint32_t>(startptr + sizeof(uint32_t));
	return container;
}

idx_t UncompressedStringStorage::RemainingSpace(ColumnSegment &segment, BufferHandle &handle) {
	auto dictionary = GetDictionary(segment, handle);
	D_ASSERT(dictionary.end == segment.SegmentSize());
	idx_t used_space = dictionary.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
	D_ASSERT(segment.SegmentSize() >= used_space);
	return segment.SegmentSize() - used_space;
}

void UncompressedStringStorage::WriteString(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                            int32_t &result_offset) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (state.overflow_writer) {
		// overflow writer is set: write string there
		state.overflow_writer->WriteString(state, string, result_block, result_offset);
	} else {
		// default overflow behavior: use in-memory buffer to store the overflow string
		WriteStringMemory(segment, string, result_block, result_offset);
	}
}

void UncompressedStringStorage::WriteStringMemory(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                                  int32_t &result_offset) {
	auto total_length = UnsafeNumericCast<uint32_t>(string.GetSize() + sizeof(uint32_t));
	shared_ptr<BlockHandle> block;
	BufferHandle handle;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
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
		state.InsertOverflowBlock(block->BlockId(), reference<StringBlock>(*new_block));
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
	auto ptr = handle.GetDataMutable() + state.head->offset;
	Store<uint32_t>(UnsafeNumericCast<uint32_t>(string.GetSize()), ptr);
	ptr += sizeof(uint32_t);
	memcpy(ptr, string.GetData(), string.GetSize());
	state.head->offset += total_length;
}

string_t UncompressedStringStorage::ReadOverflowString(const QueryContext &context, ColumnSegment &segment,
                                                       Vector &result, block_id_t block, int32_t offset) {
	auto &buffer_manager = segment.GetBlockHandle()->GetMemory().GetBufferManager();
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();

	if (block < 0) {
		ThrowInvalidOverflowStringBlock();
	}
	if (offset < 0) {
		ThrowOverflowStringOffsetOutOfBounds();
	}

	if (block < MAXIMUM_BLOCK) {
		// read the overflow string from disk
		// pin the initial handle and read the length
		auto block_handle = state.GetHandle(segment.GetBlockHandle()->GetBlockManager(), block);
		auto handle = buffer_manager.Pin(context, block_handle);

		// read header
		auto block_size = segment.GetBlockSize();
		auto string_space = block_size - sizeof(block_id_t);
		CompressionSegmentReader block_reader(handle.Ptr(), block_size, "overflow string block");
		auto reader = block_reader.GetSubReader(0, string_space, "overflow string data");
		reader.SetPosition(NumericCast<idx_t>(offset));
		uint32_t length = reader.Read<uint32_t>();
		uint32_t remaining = length;

		BufferHandle target_handle;
		string_t overflow_string;
		data_ptr_t target_ptr;
		bool allocate_block = length >= segment.GetBlockSize();
		if (allocate_block) {
			// overflow string is bigger than a block - allocate a temporary buffer for it
			target_handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, length);
			target_ptr = target_handle.GetDataMutable();
		} else {
			// overflow string is smaller than a block - add it to the vector directly
			overflow_string = StringVector::EmptyString(result, length);
			target_ptr = data_ptr_cast(overflow_string.GetDataWriteable());
		}

		// now append the string to the single buffer
		while (remaining > 0) {
			idx_t to_write = MinValue<idx_t>(remaining, reader.Remaining());
			reader.ReadBytesInto(target_ptr, to_write);
			remaining -= to_write;
			target_ptr += to_write;
			if (remaining > 0) {
				// read the next block
				block_id_t next_block = block_reader.Get<block_id_t>(string_space);
				block_handle = state.GetHandle(segment.GetBlockHandle()->GetBlockManager(), next_block);
				handle = buffer_manager.Pin(context, block_handle);
				block_reader = CompressionSegmentReader(handle.Ptr(), block_size, "overflow string block");
				reader = block_reader.GetSubReader(0, string_space, "overflow string data");
			}
		}
		if (allocate_block) {
			auto final_buffer = target_handle.Ptr();
			StringVector::AddHandle(result, std::move(target_handle));
			return string_t(const_char_ptr_cast(final_buffer), length);
		} else {
			overflow_string.Finalize();
			return overflow_string;
		}
	}

	// read the overflow string from memory
	// first pin the handle, if it is not pinned yet
	auto string_block = state.FindOverflowBlock(block);
	auto handle = buffer_manager.Pin(context, string_block.get().block);
	auto final_buffer = handle.Ptr();
	StringVector::AddHandle(result, std::move(handle));
	CompressionSegmentReader reader(final_buffer, string_block.get().offset, "in-memory overflow string block");
	return ReadStringWithLength(reader, offset);
}

string_t UncompressedStringStorage::ReadStringWithLength(CompressionSegmentReader reader, int32_t offset) {
	reader.SetPosition(NumericCast<idx_t>(offset));
	auto string_length = reader.Read<uint32_t>();
	auto string_data = reader.ReadBytes(string_length);
	return string_t(const_char_ptr_cast(string_data.data()), string_length);
}

void UncompressedStringStorage::WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

} // namespace duckdb
