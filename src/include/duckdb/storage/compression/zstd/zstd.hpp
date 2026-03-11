#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {

using page_id_t = int64_t;
using page_offset_t = uint32_t;
using uncompressed_size_t = uint64_t;
using compressed_size_t = uint64_t;
using string_length_t = uint32_t;

struct ZSTDCompressionBufferFlags {
public:
	ZSTDCompressionBufferFlags() : value(0) {
	}
	ZSTDCompressionBufferFlags(const ZSTDCompressionBufferFlags &other) : value(other.value) {
	}
	~ZSTDCompressionBufferFlags() = default;

	ZSTDCompressionBufferFlags &operator=(const ZSTDCompressionBufferFlags &other) {
		value = other.value;
		return *this;
	}

	bool operator==(const ZSTDCompressionBufferFlags &other) const {
		return other.value == value;
	}
	bool operator!=(const ZSTDCompressionBufferFlags &other) const {
		return !(*this == other);
	}

public:
	// Bit layout
	static constexpr uint8_t VECTOR_METADATA_BIT = 0;
	static constexpr uint8_t STRING_METADATA_BIT = 1;
	static constexpr uint8_t DATA_BIT = 2;

	// Getters
	bool HasVectorMetadata() const {
		return IsSet<VECTOR_METADATA_BIT>();
	}
	bool HasStringMetadata() const {
		return IsSet<STRING_METADATA_BIT>();
	}
	bool HasData() const {
		return IsSet<DATA_BIT>();
	}

	// Setters
	void SetVectorMetadata() {
		Set<VECTOR_METADATA_BIT>();
	}
	void SetStringMetadata() {
		Set<STRING_METADATA_BIT>();
	}
	void SetData() {
		Set<DATA_BIT>();
	}

	// Unsetters
	void UnsetVectorMetadata() {
		Unset<VECTOR_METADATA_BIT>();
	}
	void UnsetStringMetadata() {
		Unset<STRING_METADATA_BIT>();
	}
	void UnsetData() {
		Unset<DATA_BIT>();
	}

	// Clear all flags
	void Clear() {
		value = 0;
	}

protected:
	template <uint8_t BIT>
	bool IsSet() const {
		static const uint8_t FLAG = (1 << BIT);
		return (value & FLAG) == FLAG;
	}

	template <uint8_t BIT>
	void Set() {
		static const uint8_t FLAG = (1 << BIT);
		value |= FLAG;
	}

	template <uint8_t BIT>
	void Unset() {
		static const uint8_t FLAG = (1 << BIT);
		value &= ~FLAG;
	}

private:
	uint8_t value;
};

struct ZSTDCompressionBufferState {
	//! Flags indicating use of this buffer
	ZSTDCompressionBufferFlags flags;
	page_offset_t offset = 0;
	bool full = false;
};

struct ZSTDCompressionBufferCollection {
public:
	enum class Slot : uint8_t { SEGMENT, OVERFLOW_0, OVERFLOW_1 };

public:
	struct BufferData {
	public:
		BufferData(BufferHandle &handle, ZSTDCompressionBufferState &state, Slot slot)
		    : handle(handle), state(state), slot(slot) {
		}

	public:
		BufferHandle &handle;
		ZSTDCompressionBufferState &state;
		Slot slot;
	};

public:
	page_id_t GetCurrentId() const {
#ifdef DEBUG
		if (!buffer_index.IsValid() || buffer_index == 0) {
			D_ASSERT(block_id == INVALID_BLOCK);
		} else {
			D_ASSERT(block_id != INVALID_BLOCK);
		}
#endif
		return block_id;
	}

public:
	void SetCurrentBuffer(Slot slot, page_offset_t offset = 0) {
		idx_t index;
		switch (slot) {
		case Slot::SEGMENT:
			index = 0;
			break;
		case Slot::OVERFLOW_0:
			index = 1;
			break;
		case Slot::OVERFLOW_1:
			index = 2;
			break;
		default:
			throw InternalException("ZSTDCompressionBufferCollection::Slot value not handled");
		};
		buffer_index = index;
		buffer_states[index].offset = offset;
	}
	page_offset_t &GetCurrentOffset() {
		if (!buffer_index.IsValid()) {
			throw InternalException(
			    "(ZSTDCompressionBufferCollection::GetCurrentOffset) Can't get BufferHandle, no buffer set yet!");
		}
		auto index = buffer_index.GetIndex();
		auto &offset = buffer_states[index].offset;
		return offset;
	}
	void AlignCurrentOffset() {
		auto &offset = GetCurrentOffset();
		offset = UnsafeNumericCast<page_offset_t>(
		    AlignValue<idx_t, sizeof(string_length_t)>(UnsafeNumericCast<idx_t>(offset)));
	}
	BufferHandle &BufferHandleMutable() {
		if (!buffer_index.IsValid()) {
			throw InternalException(
			    "(ZSTDCompressionBufferCollection::BufferHandleMutable) Can't get BufferHandle, no buffer set yet!");
		}
		auto index = buffer_index.GetIndex();
		if (index == 0) {
			return segment_handle;
		}
		D_ASSERT(index < 3);
		return extra_pages[index - 1];
	}
	vector<BufferData> GetBufferData(bool include_segment) {
		vector<BufferData> res;
		for (idx_t i = 0; i < 3; i++) {
			if (!i) {
				if (include_segment) {
					res.emplace_back(segment_handle, buffer_states[i], Slot::SEGMENT);
				}
				continue;
			}
			res.emplace_back(extra_pages[i - 1], buffer_states[i], i == 1 ? Slot::OVERFLOW_0 : Slot::OVERFLOW_1);
		}
		return res;
	}
	data_ptr_t GetCurrentBufferPtr() {
		if (!buffer_index.IsValid()) {
			throw InternalException(
			    "(ZSTDCompressionBufferCollection::GetCurrentBufferPtr) Can't get BufferHandle, no buffer set yet!");
		}
		auto index = buffer_index.GetIndex();
		auto &state = buffer_states[index];
		return BufferHandleMutable().Ptr() + state.offset;
	}
	bool CanFlush() const {
		if (!buffer_index.IsValid()) {
			throw InternalException(
			    "(ZSTDCompressionBufferCollection::CanFlush) Can't determine CanFlush, no buffer set yet!");
		}
		auto index = buffer_index.GetIndex();
		if (index == 0) {
			//! Can't flush the segment buffer
			return false;
		}
		auto &flags = buffer_states[index].flags;
		return !flags.HasVectorMetadata() && !flags.HasStringMetadata();
	}
	ZSTDCompressionBufferFlags &GetCurrentFlags() {
		return GetCurrentBufferState().flags;
	}
	ZSTDCompressionBufferState &GetCurrentBufferState() {
		if (!buffer_index.IsValid()) {
			throw InternalException(
			    "(ZSTDCompressionBufferCollection::GetCurrentBufferState) Can't get BufferState, no buffer set yet!");
		}
		return buffer_states[buffer_index.GetIndex()];
	}
	bool IsOnSegmentBuffer() const {
		if (!buffer_index.IsValid()) {
			return false;
		}
		return buffer_index.GetIndex() == 0;
	}

public:
	//! Current block-id of the overflow page we're writing
	//! NOTE: INVALID_BLOCK means we haven't spilled to an overflow page yet
	block_id_t block_id = INVALID_BLOCK;

	//! The current segment + buffer of the segment
	unique_ptr<ColumnSegment> segment;
	BufferHandle segment_handle;
	// Non-segment buffers
	BufferHandle extra_pages[2];

	//! 0: segment_handle
	//! 1: extra_pages[0];
	//! 2: extra_pages[1];
	ZSTDCompressionBufferState buffer_states[3];

private:
	optional_idx buffer_index;
};

//! State for the current segment (a collection of vectors)
struct ZSTDCompressionSegmentState {
public:
	ZSTDCompressionSegmentState() {
	}

public:
	void InitializeSegment(ZSTDCompressionBufferCollection &buffer_collection, idx_t vectors_in_segment) {
		total_vectors_in_segment = vectors_in_segment;
		vector_in_segment_count = 0;
		buffer_collection.block_id = INVALID_BLOCK;

		//! Have to be on the segment handle
		if (!buffer_collection.IsOnSegmentBuffer()) {
			throw InternalException("(ZSTDCompressionSegmentState::InitializeSegment) Can't InitializeSegment on a "
			                        "non-segment buffer!");
		}
		auto base = buffer_collection.segment_handle.Ptr();
		page_offset_t offset = 0;
		page_ids = reinterpret_cast<page_id_t *>(base + offset);
		offset += (sizeof(page_id_t) * vectors_in_segment);

		offset = AlignValue<page_offset_t, sizeof(page_offset_t)>(offset);
		page_offsets = reinterpret_cast<page_offset_t *>(base + offset);
		offset += (sizeof(page_offset_t) * vectors_in_segment);

		offset = AlignValue<page_offset_t, sizeof(uncompressed_size_t)>(offset);
		uncompressed_sizes = reinterpret_cast<uncompressed_size_t *>(base + offset);
		offset += (sizeof(uncompressed_size_t) * vectors_in_segment);

		offset = AlignValue<page_offset_t, sizeof(compressed_size_t)>(offset);
		compressed_sizes = reinterpret_cast<compressed_size_t *>(base + offset);
		offset += (sizeof(compressed_size_t) * vectors_in_segment);

		buffer_collection.buffer_states[0].offset = offset;
	}

public:
	//! Amount of vectors in this segment, determined during analyze
	idx_t total_vectors_in_segment = 0xDEADBEEF;
	//! The amount of vectors we've seen in the current segment
	idx_t vector_in_segment_count = 0;

	page_id_t *page_ids = nullptr;
	page_offset_t *page_offsets = nullptr;
	uncompressed_size_t *uncompressed_sizes = nullptr;
	compressed_size_t *compressed_sizes = nullptr;
};

//===--------------------------------------------------------------------===//
// Vector metadata
//===--------------------------------------------------------------------===//
struct ZSTDCompressionVectorState {
public:
	ZSTDCompressionVectorState() {
	}

public:
	bool AddStringLength(const string_t &str) {
		string_lengths[tuple_count++] = UnsafeNumericCast<string_length_t>(str.GetSize());
		return tuple_count >= vector_size;
	}

	void Initialize(idx_t expected_tuple_count, ZSTDCompressionBufferCollection &buffer_collection,
	                const CompressionInfo &info) {
		vector_size = expected_tuple_count;

		auto current_offset = buffer_collection.GetCurrentOffset();
		//! Mark where the vector begins (page_id + page_offset)
		starting_offset = current_offset;
		starting_page = buffer_collection.GetCurrentId();

		//! Set the string_lengths destination and save in what buffer its stored
		buffer_collection.GetCurrentFlags().SetStringMetadata();
		string_lengths = reinterpret_cast<string_length_t *>(buffer_collection.GetCurrentBufferPtr());

		//! Finally forward the current_buffer_ptr to point *after* all string lengths we'll write
		buffer_collection.GetCurrentOffset() += expected_tuple_count * sizeof(string_length_t);
	}

public:
	page_id_t starting_page;
	page_offset_t starting_offset;

	idx_t uncompressed_size = 0;
	idx_t compressed_size = 0;
	string_length_t *string_lengths = nullptr;

	bool in_vector = false;
	//! Amount of tuples we have seen for the current vector
	idx_t tuple_count = 0;
	//! The expected size of this vector (ZSTD_VECTOR_SIZE except for the last one)
	idx_t vector_size;
};

} // namespace duckdb
