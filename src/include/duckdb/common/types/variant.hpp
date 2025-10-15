#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

enum class VariantChildLookupMode : uint8_t { BY_KEY, BY_INDEX };

struct VariantPathComponent {
	VariantChildLookupMode lookup_mode;
	string key;
	uint32_t index;
};

struct VariantNestedData {
	//! The amount of children in the nested structure
	uint32_t child_count;
	//! Index of the first child
	uint32_t children_idx;
	//! Whether the row is null
	bool is_null;
};

struct VariantDecimalData {
public:
	VariantDecimalData(uint32_t width, uint32_t scale, const_data_ptr_t value_ptr)
	    : width(width), scale(scale), value_ptr(value_ptr) {
	}

public:
	PhysicalType GetPhysicalType() const;

public:
	uint32_t width;
	uint32_t scale;
	const_data_ptr_t value_ptr = nullptr;
};

struct VariantVectorData {
public:
	explicit VariantVectorData(Vector &variant)
	    : variant(variant), keys_index_validity(FlatVector::Validity(VariantVector::GetChildrenKeysIndex(variant))),
	      keys(VariantVector::GetKeys(variant)) {
		blob_data = FlatVector::GetData<string_t>(VariantVector::GetData(variant));
		type_ids_data = FlatVector::GetData<uint8_t>(VariantVector::GetValuesTypeId(variant));
		byte_offset_data = FlatVector::GetData<uint32_t>(VariantVector::GetValuesByteOffset(variant));
		keys_index_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenKeysIndex(variant));
		values_index_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenValuesIndex(variant));
		values_data = FlatVector::GetData<list_entry_t>(VariantVector::GetValues(variant));
		children_data = FlatVector::GetData<list_entry_t>(VariantVector::GetChildren(variant));
		keys_data = FlatVector::GetData<list_entry_t>(keys);
	}

public:
	Vector &variant;

	//! value
	string_t *blob_data;

	//! values
	uint8_t *type_ids_data;
	uint32_t *byte_offset_data;

	//! children
	uint32_t *keys_index_data;
	uint32_t *values_index_data;
	ValidityMask &keys_index_validity;

	//! values | children | keys
	list_entry_t *values_data;
	list_entry_t *children_data;
	list_entry_t *keys_data;

	Vector &keys;
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
	BIGNUM = 31,
	BITSTRING = 32,
	GEOMETRY = 33,
	ENUM_SIZE /* always kept as last item of the enum */
};

struct UnifiedVariantVectorData {
public:
	explicit UnifiedVariantVectorData(const RecursiveUnifiedVectorFormat &variant)
	    : variant(variant), keys(UnifiedVariantVector::GetKeys(variant)),
	      keys_entry(UnifiedVariantVector::GetKeysEntry(variant)), children(UnifiedVariantVector::GetChildren(variant)),
	      keys_index(UnifiedVariantVector::GetChildrenKeysIndex(variant)),
	      values_index(UnifiedVariantVector::GetChildrenValuesIndex(variant)),
	      values(UnifiedVariantVector::GetValues(variant)), type_id(UnifiedVariantVector::GetValuesTypeId(variant)),
	      byte_offset(UnifiedVariantVector::GetValuesByteOffset(variant)), data(UnifiedVariantVector::GetData(variant)),
	      keys_index_validity(keys_index.validity) {
		blob_data = data.GetData<string_t>();
		type_id_data = type_id.GetData<uint8_t>();
		byte_offset_data = byte_offset.GetData<uint32_t>();
		keys_index_data = keys_index.GetData<uint32_t>();
		values_index_data = values_index.GetData<uint32_t>();
		values_data = values.GetData<list_entry_t>();
		children_data = children.GetData<list_entry_t>();
		keys_data = keys.GetData<list_entry_t>();
		keys_entry_data = keys_entry.GetData<string_t>();
	}

public:
	bool RowIsValid(idx_t row) const {
		return variant.unified.validity.RowIsValid(variant.unified.sel->get_index(row));
	}
	bool KeysIndexIsValid(idx_t row, idx_t index) const {
		auto list_entry = GetChildrenListEntry(row);
		return keys_index_validity.RowIsValid(keys_index.sel->get_index(list_entry.offset + index));
	}

	list_entry_t GetChildrenListEntry(idx_t row) const {
		return children_data[children.sel->get_index(row)];
	}
	list_entry_t GetValuesListEntry(idx_t row) const {
		return values_data[values.sel->get_index(row)];
	}
	const string_t &GetKey(idx_t row, idx_t index) const {
		auto list_entry = keys_data[keys.sel->get_index(row)];
		return keys_entry_data[keys_entry.sel->get_index(list_entry.offset + index)];
	}
	uint32_t GetKeysIndex(idx_t row, idx_t child_index) const {
		auto list_entry = GetChildrenListEntry(row);
		return keys_index_data[keys_index.sel->get_index(list_entry.offset + child_index)];
	}
	uint32_t GetValuesIndex(idx_t row, idx_t child_index) const {
		auto list_entry = GetChildrenListEntry(row);
		return values_index_data[values_index.sel->get_index(list_entry.offset + child_index)];
	}
	VariantLogicalType GetTypeId(idx_t row, idx_t value_index) const {
		auto list_entry = values_data[values.sel->get_index(row)];
		return static_cast<VariantLogicalType>(type_id_data[type_id.sel->get_index(list_entry.offset + value_index)]);
	}
	uint32_t GetByteOffset(idx_t row, idx_t value_index) const {
		auto list_entry = values_data[values.sel->get_index(row)];
		return byte_offset_data[byte_offset.sel->get_index(list_entry.offset + value_index)];
	}
	const string_t &GetData(idx_t row) const {
		return blob_data[data.sel->get_index(row)];
	}

public:
	const RecursiveUnifiedVectorFormat &variant;
	const UnifiedVectorFormat &keys;
	const UnifiedVectorFormat &keys_entry;
	const UnifiedVectorFormat &children;
	const UnifiedVectorFormat &keys_index;
	const UnifiedVectorFormat &values_index;
	const UnifiedVectorFormat &values;
	const UnifiedVectorFormat &type_id;
	const UnifiedVectorFormat &byte_offset;
	const UnifiedVectorFormat &data;

	const list_entry_t *keys_data = nullptr;
	const string_t *keys_entry_data = nullptr;
	const list_entry_t *children_data = nullptr;
	const uint32_t *keys_index_data = nullptr;
	const uint32_t *values_index_data = nullptr;
	const list_entry_t *values_data = nullptr;
	const uint8_t *type_id_data = nullptr;
	const uint32_t *byte_offset_data = nullptr;
	const string_t *blob_data = nullptr;

	const ValidityMask &keys_index_validity;
};

struct VariantCasts {
	static duckdb_yyjson::yyjson_mut_val *ConvertVariantToJSON(duckdb_yyjson::yyjson_mut_doc *doc,
	                                                           const RecursiveUnifiedVectorFormat &source, idx_t row,
	                                                           uint32_t values_idx);
};

} // namespace duckdb
