#pragma once

#include "duckdb/common/types/string_type.hpp"

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
	idx_t offsets_length;
	const_data_ptr_t bytes;
};

//! ------------ Value ------------

enum class VariantBasicType : uint8_t { PRIMITIVE = 0, SHORT_STRING = 1, OBJECT = 2, ARRAY = 3 };

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
	TIME_NTZ = 17,
	TIMESTAMP_NANOS = 18,
	TIMESTAMP_NTZ_NANOS = 19,
	UUID = 20
};

static VariantBasicType VariantBasicTypeFromByte(uint8_t byte) {
	switch (byte) {
	case 0:
		return VariantBasicType::PRIMITIVE;
	case 1:
		return VariantBasicType::SHORT_STRING;
	case 2:
		return VariantBasicType::OBJECT;
	case 3:
		return VariantBasicType::ARRAY;
	default:
		throw NotImplementedException("Variant BasicType (%d) is not supported", byte);
	}
}

struct VariantValueMetadata {
public:
	VariantValueMetadata() {
	}

public:
	static VariantValueMetadata FromHeaderByte(uint8_t byte);

public:
	VariantBasicType basic_type;
	uint8_t header;
};

struct VariantValue {
public:
	VariantValue() {
	}

public:
};

class VariantBinaryDecoder {
public:
	VariantBinaryDecoder();

public:
	string_t Decode(const string_t &metadata, const string_t &blob);

public:
	string_t PrimitiveTypeDecode(const VariantMetadata &metadata, const VariantValueMetadata &value_metadata,
	                             const string_t &blob);
	string_t ShortStringDecode(const VariantMetadata &metadata, const VariantValueMetadata &value_metadata,
	                           const string_t &blob);
	string_t ObjectDecode(const VariantMetadata &metadata, const VariantValueMetadata &value_metadata,
	                      const string_t &blob);
	string_t ArrayDecode(const VariantMetadata &metadata, const VariantValueMetadata &value_metadata,
	                     const string_t &blob);
};

} // namespace duckdb
