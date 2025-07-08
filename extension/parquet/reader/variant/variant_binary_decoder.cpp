#include "reader/variant/variant_binary_decoder.hpp"
#include "duckdb/common/printer.hpp"
#include "utf8proc_wrapper.hpp"

static constexpr uint8_t VERSION_MASK = 0xF;
static constexpr uint8_t SORTED_STRINGS_MASK = 0x1;
static constexpr uint8_t SORTED_STRINGS_SHIFT = 4;
static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_MASK = 0x3;
static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_SHIFT = 5;

static constexpr uint8_t BASIC_TYPE_MASK = 0x1;
static constexpr uint8_t VALUE_HEADER_MASK = 0x2;
static constexpr uint8_t VALUE_HEADER_SHIFT = 1;

//! Object and Array header
static constexpr uint8_t FIELD_OFFSET_SIZE_MINUS_ONE_MASK = 0x3;

//! Object header
static constexpr uint8_t FIELD_ID_SIZE_MINUS_ONE_MASK = 0x3;
static constexpr uint8_t FIELD_ID_SIZE_MINUS_ONE_SHIFT = 2;

static constexpr uint8_t OBJECT_IS_LARGE_MASK = 0x1;
static constexpr uint8_t OBJECT_IS_LARGE_SHIFT = 4;

//! Array header
static constexpr uint8_t ARRAY_IS_LARGE_MASK = 0x1;
static constexpr uint8_t ARRAY_IS_LARGE_SHIFT = 2;

using namespace duckdb_yyjson;

namespace duckdb {

namespace {

static idx_t ReadVariableLengthLittleEndian(idx_t length_in_bytes, const_data_ptr_t &ptr) {
	if (length_in_bytes > sizeof(idx_t)) {
		throw NotImplementedException("Can't read little-endian value of %d bytes", length_in_bytes);
	}
	idx_t result = 0;
	memcpy(reinterpret_cast<uint8_t *>(&result), ptr, length_in_bytes);
	ptr += length_in_bytes;
	return result;
}

} // namespace

VariantMetadataHeader VariantMetadataHeader::FromHeaderByte(uint8_t byte) {
	VariantMetadataHeader header;
	header.version = byte & VERSION_MASK;
	header.sorted_strings = (byte >> SORTED_STRINGS_SHIFT) & SORTED_STRINGS_MASK;
	header.offset_size = ((byte >> OFFSET_SIZE_MINUS_ONE_SHIFT) & OFFSET_SIZE_MINUS_ONE_MASK) + 1;

	if (header.version != 1) {
		throw NotImplementedException("Only version 1 of the Variant encoding scheme is supported, found version: %d",
		                              header.version);
	}

	return header;
}

VariantMetadata::VariantMetadata(const string_t &metadata) : metadata(metadata) {
	auto metadata_length = metadata.GetSize();
	auto metadata_data = metadata.GetData();

	header = VariantMetadataHeader::FromHeaderByte(metadata_data[0]);

	const_data_ptr_t ptr = reinterpret_cast<const_data_ptr_t>(metadata_data + sizeof(uint8_t));
	idx_t dictionary_size = ReadVariableLengthLittleEndian(header.offset_size, ptr);

	offsets = ptr;
	bytes = offsets + ((dictionary_size + 1) * header.offset_size);
	idx_t last_offset = ReadVariableLengthLittleEndian(header.offset_size, ptr);
	for (idx_t i = 0; i < dictionary_size; i++) {
		auto next_offset = ReadVariableLengthLittleEndian(header.offset_size, ptr);
		Printer::PrintF("Offset[%d] = %d", i, last_offset);
		strings.push_back(reinterpret_cast<const char *>(bytes + last_offset));
		lengths.push_back(next_offset - last_offset);
		last_offset = next_offset;
	}
}

VariantValueMetadata VariantValueMetadata::FromHeaderByte(uint8_t byte) {
	VariantValueMetadata result;
	result.basic_type = VariantBasicTypeFromByte(byte & BASIC_TYPE_MASK);
	switch (result.basic_type) {
	case VariantBasicType::PRIMITIVE: {
		result.primitive_type = VariantPrimitiveTypeFromByte((byte >> VALUE_HEADER_SHIFT) & VALUE_HEADER_MASK);
		break;
	}
	case VariantBasicType::SHORT_STRING: {
		result.string_size = (byte >> VALUE_HEADER_SHIFT) & VALUE_HEADER_MASK;
		break;
	}
	case VariantBasicType::OBJECT: {
		result.field_offset_size = (byte & FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
		result.field_id_size = ((byte >> FIELD_ID_SIZE_MINUS_ONE_SHIFT) & FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
		result.is_large = (byte >> OBJECT_IS_LARGE_SHIFT) & OBJECT_IS_LARGE_MASK;
		break;
	}
	case VariantBasicType::ARRAY: {
		result.field_offset_size = (byte & FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
		result.is_large = (byte >> ARRAY_IS_LARGE_SHIFT) & ARRAY_IS_LARGE_MASK;
		break;
	}
	}
	return result;
}

VariantBinaryDecoder::VariantBinaryDecoder() {
}

yyjson_mut_val *VariantBinaryDecoder::PrimitiveTypeDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                          const VariantValueMetadata &value_metadata,
                                                          const_data_ptr_t data) {
	switch (value_metadata.primitive_type) {
	case VariantPrimitiveType::NULL_TYPE: {
		return yyjson_mut_null(doc);
	}
	case VariantPrimitiveType::BOOLEAN_TRUE: {
		return yyjson_mut_true(doc);
	}
	case VariantPrimitiveType::BOOLEAN_FALSE: {
		return yyjson_mut_false(doc);
	}
	case VariantPrimitiveType::INT8: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::INT8");
	}
	case VariantPrimitiveType::INT16: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::INT16");
	}
	case VariantPrimitiveType::INT32: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::INT32");
	}
	case VariantPrimitiveType::INT64: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::INT64");
	}
	case VariantPrimitiveType::DOUBLE: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::DOUBLE");
	}
	case VariantPrimitiveType::DECIMAL4: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::DECIMAL4");
	}
	case VariantPrimitiveType::DECIMAL8: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::DECIMAL8");
	}
	case VariantPrimitiveType::DECIMAL16: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::DECIMAL16");
	}
	case VariantPrimitiveType::DATE: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::DATE");
	}
	case VariantPrimitiveType::TIMESTAMP_MICROS: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::TIMESTAMP_MICROS");
	}
	case VariantPrimitiveType::TIMESTAMP_NTZ_MICROS: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::TIMESTAMP_NTZ_MICROS");
	}
	case VariantPrimitiveType::FLOAT: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::FLOAT");
	}
	case VariantPrimitiveType::BINARY: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::BINARY");
	}
	case VariantPrimitiveType::STRING: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::STRING");
	}
	case VariantPrimitiveType::TIME_NTZ: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::TIME_NTZ");
	}
	case VariantPrimitiveType::TIMESTAMP_NANOS: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::TIMESTAMP_NANOS");
	}
	case VariantPrimitiveType::TIMESTAMP_NTZ_NANOS: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::TIMESTAMP_NTZ_NANOS");
	}
	case VariantPrimitiveType::UUID: {
		auto val = yyjson_mut_obj(doc);
		throw NotImplementedException("PrimitiveTypeDecode for VariantPrimitiveType::UUID");
	}
	default:
		throw NotImplementedException("Variant PrimitiveTypeDecode not implemented for type (%d)",
		                              static_cast<uint8_t>(value_metadata.primitive_type));
	}
}

yyjson_mut_val *VariantBinaryDecoder::ShortStringDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                        const VariantValueMetadata &value_metadata,
                                                        const_data_ptr_t data) {
	D_ASSERT(value_metadata.string_size < 64);
	auto string_data = reinterpret_cast<const char *>(data);
	if (!Utf8Proc::IsValid(string_data, value_metadata.string_size)) {
		throw InternalException("Can't decode Variant short-string, string isn't valid UTF8");
	}
	return yyjson_mut_strncpy(doc, string_data, value_metadata.string_size);
}

yyjson_mut_val *VariantBinaryDecoder::ObjectDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                   const VariantValueMetadata &value_metadata, const_data_ptr_t data) {
	auto obj = yyjson_mut_obj(doc);

	auto field_offset_size = value_metadata.field_offset_size;
	auto field_id_size = value_metadata.field_id_size;
	auto is_large = value_metadata.is_large;

	idx_t num_elements;
	if (is_large) {
		num_elements = Load<uint32_t>(data);
		data += sizeof(uint32_t);
	} else {
		num_elements = Load<uint8_t>(data);
		data += sizeof(uint8_t);
	}

	auto field_ids = data;
	auto field_offsets = data + (num_elements * field_id_size);
	auto values = field_offsets + ((num_elements + 1) * field_offset_size);

	idx_t last_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);
	for (idx_t i = 0; i < num_elements; i++) {
		auto field_id = ReadVariableLengthLittleEndian(field_id_size, field_ids);
		auto next_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);

		auto value = Decode(doc, metadata, values + last_offset);
		last_offset = next_offset;
	}

	throw NotImplementedException("VariantBinaryDecoder::ObjectDecode");
	return obj;
}

yyjson_mut_val *VariantBinaryDecoder::ArrayDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                  const VariantValueMetadata &value_metadata, const_data_ptr_t data) {
	auto arr = yyjson_mut_arr(doc);

	auto field_offset_size = value_metadata.field_offset_size;
	auto is_large = value_metadata.is_large;

	uint32_t num_elements;
	if (is_large) {
		num_elements = Load<uint32_t>(data);
		data += sizeof(uint32_t);
	} else {
		num_elements = Load<uint8_t>(data);
		data += sizeof(uint8_t);
	}

	auto field_offsets = data;
	auto values = field_offsets + ((num_elements + 1) * field_offset_size);

	idx_t last_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);
	for (idx_t i = 0; i < num_elements; i++) {
		auto next_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);

		auto value = Decode(doc, metadata, values + last_offset);
		last_offset = next_offset;
	}

	throw NotImplementedException("Variant ArrayDecode");
	return arr;
}

yyjson_mut_val *VariantBinaryDecoder::Decode(yyjson_mut_doc *doc, const VariantMetadata &variant_metadata,
                                             const_data_ptr_t data) {
	auto value_metadata = VariantValueMetadata::FromHeaderByte(data[0]);

	//! FIXME: we don't actually know the length in arrays/objects?
	//! > A field_offset represents the byte offset (relative to the first byte of the first value) where the i-th value
	//! starts > The last field_offset points to the byte after the end of the last value
	//! ...
	//! > This implies that the field_offset values may not be monotonically increasing

	switch (value_metadata.basic_type) {
	case VariantBasicType::PRIMITIVE: {
		return PrimitiveTypeDecode(doc, variant_metadata, value_metadata, data);
	}
	case VariantBasicType::SHORT_STRING: {
		return ShortStringDecode(doc, variant_metadata, value_metadata, data);
	}
	case VariantBasicType::OBJECT: {
		return ObjectDecode(doc, variant_metadata, value_metadata, data);
	}
	case VariantBasicType::ARRAY: {
		return ArrayDecode(doc, variant_metadata, value_metadata, data);
	}
	default:
		throw InternalException("Unexpected value for VariantBasicType");
	}
}

} // namespace duckdb
