#include "duckdb/common/serializer/message_serializer.hpp"

namespace duckdb {

MessageSerializer::MessageSerializer(MemoryStream &stream, SerializationOptions options_p)
    : BinarySerializer(stream, std::move(options_p)), memory_stream(stream) {
}

void MessageSerializer::BeginProperty(const field_id_t field_id, const char *tag) {
	OnPropertyBegin(field_id, tag);
}

void MessageSerializer::EndProperty() {
	OnPropertyEnd();
}

MessageSerializer::ReservedSlot MessageSerializer::ReserveVarint(idx_t width, uint64_t initial_value) {
	if (EncodingUtil::MinimalLEB128Width(initial_value) > width) {
		throw SerializationException("Failed to serialize: initial value %llu does not fit in a reserved slot of "
		                             "%llu bytes",
		                             initial_value, width);
	}
	ReservedSlot slot;
	slot.offset = memory_stream.GetPosition();
	slot.width = width;

	// The placeholder is the value of an ordinary field, only written wide. No framing is bypassed.
	data_t padded[EncodingUtil::MaxLEB128Width<uint64_t>()];
	EncodingUtil::EncodePaddedLEB128<uint64_t>(padded, initial_value, width);
	memory_stream.WriteData(padded, width);
	return slot;
}

MessageSerializer::ReservedSlot MessageSerializer::BeginDeferredList(const field_id_t field_id, const char *tag) {
	BeginProperty(field_id, tag);
	// OnListBegin would write the count a second time. The padded varint is that count.
	return ReserveVarint(EncodingUtil::MaxLEB128Width<idx_t>(), 0);
}

void MessageSerializer::EndDeferredList(ReservedSlot &slot) {
	PatchReserved(slot, slot.count);
	OnListEnd();
	EndProperty();
}

void MessageSerializer::PatchReserved(const ReservedSlot &slot, uint64_t value) {
	// Resolved now, not at reserve time, because MemoryStream moves its buffer when it grows
	PatchSlot(memory_stream.GetData(), memory_stream.GetPosition(), slot, value);
}

void MessageSerializer::PatchSlot(data_ptr_t payload, idx_t payload_size, const ReservedSlot &slot, uint64_t value) {
	if (!slot.IsValid()) {
		throw SerializationException("Failed to serialize: cannot patch an unreserved slot");
	}
	if (slot.offset + slot.width > payload_size) {
		throw SerializationException("Failed to serialize: slot at offset %llu is %llu bytes, but the payload is "
		                             "only %llu bytes",
		                             slot.offset, slot.width, payload_size);
	}
	if (EncodingUtil::MinimalLEB128Width(value) > slot.width) {
		throw SerializationException("Failed to serialize: value %llu does not fit in a reserved slot of %llu bytes",
		                             value, slot.width);
	}
	EncodingUtil::EncodePaddedLEB128<uint64_t>(payload + slot.offset, value, slot.width);
}

} // namespace duckdb
