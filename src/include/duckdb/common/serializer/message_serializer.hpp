//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/message_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/encoding_util.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

//! Writes one message step by step, instead of in a single Serialize() call. A value that is only
//! known later becomes a fixed-width padded varint, patched in place. This needs a MemoryStream,
//! because the bytes must stay available until the patch.
class MessageSerializer : public BinarySerializer {
public:
	//! A fixed-width varint in the written bytes. Plain data, so the owner of a finished payload can
	//! patch it after the serializer is gone.
	struct ReservedSlot {
		//! An offset and not a pointer, because MemoryStream moves its buffer when it grows
		idx_t offset = DConstants::INVALID_INDEX;
		idx_t width = 0;
		//! Deferred lists only
		idx_t count = 0;

		bool IsValid() const {
			return offset != DConstants::INVALID_INDEX;
		}
	};

public:
	explicit MessageSerializer(MemoryStream &stream, SerializationOptions options = SerializationOptions());

public:
	//! Field framing, public so a message can be written one field at a time
	void BeginProperty(const field_id_t field_id, const char *tag);
	void EndProperty();

	//! A property whose value is only known later. The slot is as wide as T can need, so the value
	//! cannot outgrow it. Until you patch it, the field reads back as `initial_value`.
	template <class T>
	ReservedSlot ReserveProperty(const field_id_t field_id, const char *tag, uint64_t initial_value = 0) {
		BeginProperty(field_id, tag);
		auto slot = ReserveVarint(EncodingUtil::MaxLEB128Width<T>(), initial_value);
		EndProperty();
		return slot;
	}

	//! A list whose element count is only known after the elements are written
	ReservedSlot BeginDeferredList(const field_id_t field_id, const char *tag);
	//! Writes one element with the same bytes as WriteValue(const vector<T> &) writes for one of its own
	template <class T>
	void AppendElement(ReservedSlot &slot, const T &value) {
		WriteValue(value);
		slot.count++;
	}
	void EndDeferredList(ReservedSlot &slot);

	//! Patch a slot while this serializer is alive
	void PatchReserved(const ReservedSlot &slot, uint64_t value);
	//! Patch a slot in a payload this serializer no longer owns
	static void PatchSlot(data_ptr_t payload, idx_t payload_size, const ReservedSlot &slot, uint64_t value);

private:
	ReservedSlot ReserveVarint(idx_t width, uint64_t initial_value);

private:
	//! BinarySerializer declares one overload per primitive type. In a derived class these hide the
	//! container and pointer templates of Serializer, which AppendElement needs.
	using Serializer::WriteValue;

	MemoryStream &memory_stream;
};

} // namespace duckdb
