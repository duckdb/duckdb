#pragma once

#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "reader/variant/variant_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

//! ------------ Metadata ------------

struct VariantMetadataHeader {
public:
	static VariantMetadataHeader FromHeaderByte(uint8_t byte);

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
	explicit VariantMetadata(const string_t &metadata);

public:
	const string_t &metadata;

public:
	VariantMetadataHeader header;
	const_data_ptr_t offsets;
	const_data_ptr_t bytes;

	//! The json object keys have to be null-terminated
	//! But we don't receive them null-terminated
	vector<string> strings;
};

//! ------------ Value ------------

enum class VariantBasicType : uint8_t { PRIMITIVE = 0, SHORT_STRING = 1, OBJECT = 2, ARRAY = 3, INVALID };

enum class VariantPrimitiveType : uint8_t {
	NULL_TYPE = 0,
	BOOLEAN_TRUE = 1,
	BOOLEAN_FALSE = 2,
	INT8 = 3,
	INT16 = 4,
	INT32 = 5,
	INT64 = 6,
	DOUBLE = 7,
	DECIMAL4 = 8,
	DECIMAL8 = 9,
	DECIMAL16 = 10,
	DATE = 11,
	TIMESTAMP_MICROS = 12,
	TIMESTAMP_NTZ_MICROS = 13,
	FLOAT = 14,
	BINARY = 15,
	STRING = 16,
	TIME_NTZ_MICROS = 17,
	TIMESTAMP_NANOS = 18,
	TIMESTAMP_NTZ_NANOS = 19,
	UUID = 20,
	INVALID
};

struct VariantValueMetadata {
public:
	VariantValueMetadata() {
	}

public:
	static VariantValueMetadata FromHeaderByte(uint8_t byte);
	static VariantBasicType VariantBasicTypeFromByte(uint8_t byte) {
		if (byte >= static_cast<uint8_t>(VariantBasicType::INVALID)) {
			throw NotImplementedException("Variant BasicType (%d) is not supported", byte);
		}
		return static_cast<VariantBasicType>(byte);
	}

	static VariantPrimitiveType VariantPrimitiveTypeFromByte(uint8_t byte) {
		if (byte >= static_cast<uint8_t>(VariantPrimitiveType::INVALID)) {
			throw NotImplementedException("Variant PrimitiveType (%d) is not supported", byte);
		}
		return static_cast<VariantPrimitiveType>(byte);
	}

public:
	VariantBasicType basic_type;

public:
	//! Primitive Type header
	VariantPrimitiveType primitive_type;

public:
	//! Short String header
	uint8_t string_size;

public:
	//! Object header | Array header

	//! Size in bytes for each 'field_offset' entry
	uint32_t field_offset_size;
	//! Size in bytes for each 'field_id' entry
	uint32_t field_id_size;
	//! Whether the number of elements is encoded in 1 byte (false) or 4 bytes (true)
	bool is_large;
};

struct VariantDecodeResult {
public:
	VariantDecodeResult() = default;
	~VariantDecodeResult() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
		if (data) {
			free(data);
		}
	}

public:
	yyjson_mut_doc *doc = nullptr;
	char *data = nullptr;
};

class VariantBinaryDecoder {
public:
	VariantBinaryDecoder() = delete;

public:
	static VariantValue Decode(const VariantMetadata &metadata, const_data_ptr_t data);

public:
	static VariantValue PrimitiveTypeDecode(const VariantValueMetadata &value_metadata, const_data_ptr_t data);
	static VariantValue ShortStringDecode(const VariantValueMetadata &value_metadata, const_data_ptr_t data);
	static VariantValue ObjectDecode(const VariantMetadata &metadata, const VariantValueMetadata &value_metadata,
	                                 const_data_ptr_t data);
	static VariantValue ArrayDecode(const VariantMetadata &metadata, const VariantValueMetadata &value_metadata,
	                                const_data_ptr_t data);
};

} // namespace duckdb
