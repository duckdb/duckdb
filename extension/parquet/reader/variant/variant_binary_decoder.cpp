#include "reader/variant/variant_binary_decoder.hpp"
#include "duckdb/common/printer.hpp"

static constexpr uint8_t VERSION_MASK = 0xF;

static constexpr uint8_t SORTED_STRINGS_MASK = 0x1;
static constexpr uint8_t SORTED_STRINGS_SHIFT = 4;

static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_MASK = 0x3;
static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_SHIFT = 5;

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

struct VariantMetadataHeader {
public:
	static VariantMetadataHeader FromHeaderByte(uint8_t byte) {
		VariantMetadataHeader header;
		header.version = byte & VERSION_MASK;
		header.sorted_strings = (byte >> SORTED_STRINGS_SHIFT) & SORTED_STRINGS_MASK;
		header.offset_size = ((byte >> OFFSET_SIZE_MINUS_ONE_SHIFT) & OFFSET_SIZE_MINUS_ONE_MASK) + 1;
		return header;
	}

public:
	//! The version of the protocol used (only '1' supported for now)
	uint8_t version;
	//! Number of bytes per dictionary size and offset field
	uint8_t offset_size;
	//! Whether dictionary strings are sorted and unique
	bool sorted_strings = false;
};

struct VariantMetadata {
public:
	explicit VariantMetadata(const string_t &metadata) : metadata(metadata) {
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

public:
	const string_t &metadata;

public:
	VariantMetadataHeader header;
	const_data_ptr_t offsets;
	idx_t offsets_length;
	const_data_ptr_t bytes;
};

} // namespace

VariantBinaryDecoder::VariantBinaryDecoder() {
}

string_t VariantBinaryDecoder::Decode(const string_t &metadata, const string_t &blob) {
	VariantMetadata variant_metadata(metadata);

	return string_t();
}

} // namespace duckdb
