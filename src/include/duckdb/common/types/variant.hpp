#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

enum class VariantChildLookupMode : uint8_t { BY_KEY, BY_INDEX };

struct VariantPathComponent {
	VariantChildLookupMode lookup_mode;
	union {
		string_t key;
		uint32_t index;
	} payload;
};

struct VariantNestedData {
	//! The amount of children in the nested structure
	uint32_t child_count;
	//! Index of the first child
	uint32_t children_idx;
};

enum class VariantLogicalType : uint8_t {
	VARIANT_NULL = 0,
	BOOL_TRUE = 1,
	BOOL_FALSE = 2,
	INT8 = 3,
	INT16 = 4,
	INT32 = 5,
	INT64 = 6,
	INT128 = 7,
	UINT8 = 8,
	UINT16 = 9,
	UINT32 = 10,
	UINT64 = 11,
	UINT128 = 12,
	FLOAT = 13,
	DOUBLE = 14,
	DECIMAL = 15,
	VARCHAR = 16,
	BLOB = 17,
	UUID = 18,
	DATE = 19,
	TIME_MICROS = 20,
	TIME_NANOS = 21,
	TIMESTAMP_SEC = 22,
	TIMESTAMP_MILIS = 23,
	TIMESTAMP_MICROS = 24,
	TIMESTAMP_NANOS = 25,
	TIME_MICROS_TZ = 26,
	TIMESTAMP_MICROS_TZ = 27,
	INTERVAL = 28,
	OBJECT = 29,
	ARRAY = 30,
	VARINT = 31,
	BITSTRING = 32
};

} // namespace duckdb
