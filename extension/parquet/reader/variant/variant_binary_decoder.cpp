#include "reader/variant/variant_binary_decoder.hpp"
#include "duckdb/common/printer.hpp"

static constexpr uint8_t VERSION_MASK = 0xF;
static constexpr uint8_t SORTED_STRINGS_MASK = 0x1;
static constexpr uint8_t SORTED_STRINGS_SHIFT = 4;
static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_MASK = 0x3;
static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_SHIFT = 5;

static constexpr uint8_t BASIC_TYPE_MASK = 0x1;
static constexpr uint8_t VALUE_HEADER_MASK = 0x2;
static constexpr uint8_t VALUE_HEADER_SHIFT = 1;

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
	offsets_length = dictionary_size + 1;
	for (idx_t i = 0; i < offsets_length; i++) {
		auto offset = ReadVariableLengthLittleEndian(header.offset_size, ptr);
		Printer::PrintF("Offset[%d] = %d", i, offset);
	}
	bytes = ptr;
}

VariantValueMetadata VariantValueMetadata::FromHeaderByte(uint8_t byte) {
	VariantValueMetadata result;
	result.basic_type = VariantBasicTypeFromByte(byte & BASIC_TYPE_MASK);
	result.header = (byte >> VALUE_HEADER_SHIFT) & VALUE_HEADER_MASK;
	return result;
}

VariantBinaryDecoder::VariantBinaryDecoder() {
}

yyjson_mut_val *VariantBinaryDecoder::PrimitiveTypeDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                          const VariantValueMetadata &value_metadata,
                                                          const string_t &blob) {
	throw NotImplementedException("VariantBinaryDecoder::PrimitiveTypeDecode");
}

yyjson_mut_val *VariantBinaryDecoder::ShortStringDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                        const VariantValueMetadata &value_metadata,
                                                        const string_t &blob) {
	throw NotImplementedException("VariantBinaryDecoder::ShortStringDecode");
}

yyjson_mut_val *VariantBinaryDecoder::ObjectDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                   const VariantValueMetadata &value_metadata, const string_t &blob) {
	throw NotImplementedException("VariantBinaryDecoder::ObjectDecode");
}

yyjson_mut_val *VariantBinaryDecoder::ArrayDecode(yyjson_mut_doc *doc, const VariantMetadata &metadata,
                                                  const VariantValueMetadata &value_metadata, const string_t &blob) {
	throw NotImplementedException("VariantBinaryDecoder::ArrayDecode");
}

VariantDecodeResult VariantBinaryDecoder::Decode(const string_t &metadata, const string_t &blob) {
	VariantMetadata variant_metadata(metadata);

	auto value_length = blob.GetSize();
	auto value_data = blob.GetData();
	auto value_metadata = VariantValueMetadata::FromHeaderByte(value_data[0]);

	VariantDecodeResult result;
	result.doc = yyjson_mut_doc_new(nullptr);
	auto root_obj = yyjson_mut_obj(result.doc);

	switch (value_metadata.basic_type) {
	case VariantBasicType::PRIMITIVE: {
		auto decode_result = PrimitiveTypeDecode(result.doc, variant_metadata, value_metadata, blob);
		break;
	}
	case VariantBasicType::SHORT_STRING: {
		auto decode_result = ShortStringDecode(result.doc, variant_metadata, value_metadata, blob);
		break;
	}
	case VariantBasicType::OBJECT: {
		auto decode_result = ObjectDecode(result.doc, variant_metadata, value_metadata, blob);
		break;
	}
	case VariantBasicType::ARRAY: {
		auto decode_result = ArrayDecode(result.doc, variant_metadata, value_metadata, blob);
		break;
	}
	}
	return result;
}

} // namespace duckdb
