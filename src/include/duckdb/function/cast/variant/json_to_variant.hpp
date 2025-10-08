#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
namespace variant {

using namespace duckdb_yyjson; // NOLINT

namespace {

struct ReadJSONHolder {
public:
	~ReadJSONHolder() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
	void Reset() {
		yyjson_doc_free(doc);
		doc = nullptr;
	}

public:
	yyjson_doc *doc = nullptr;
};

} // namespace

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertJSON(yyjson_val *val, ToVariantGlobalResultData &result, idx_t result_index, bool is_root);

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertJSONArray(yyjson_val *arr, ToVariantGlobalResultData &result, idx_t result_index, bool is_root) {
	yyjson_arr_iter iter;
	yyjson_arr_iter_init(arr, &iter);

	auto &variant = result.variant;

	auto children_offset_data = OffsetData::GetChildren(result.offsets);
	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);

	WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index], nullptr,
	                                 0, VariantLogicalType::ARRAY);

	auto &children_list_entry = variant.children_data[result_index];
	uint32_t count = NumericCast<uint32_t>(iter.max);
	auto start_child_index = children_list_entry.offset + children_offset_data[result_index];
	WriteContainerData<WRITE_DATA>(result.variant, result_index, blob_offset_data[result_index], count,
	                               children_offset_data[result_index]);

	//! Reserve these indices for the array
	children_offset_data[result_index] += count;

	//! Iterate over all the children in the Array
	while (yyjson_arr_iter_has_next(&iter)) {
		auto val = yyjson_arr_iter_next(&iter);

		if (WRITE_DATA) {
			//! Set the child index
			variant.keys_index_validity.SetInvalid(start_child_index);
			variant.values_index_data[start_child_index++] = values_offset_data[result_index];
		}
		if (!ConvertJSON<WRITE_DATA, IGNORE_NULLS>(val, result, result_index, false)) {
			return false;
		}
	}
	return true;
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertJSONObject(yyjson_val *obj, ToVariantGlobalResultData &result, idx_t result_index, bool is_root) {
	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);

	auto keys_offset_data = OffsetData::GetKeys(result.offsets);
	auto children_offset_data = OffsetData::GetChildren(result.offsets);
	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);

	WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index], nullptr,
	                                 0, VariantLogicalType::OBJECT);

	auto &variant = result.variant;
	auto &children_list_entry = variant.children_data[result_index];
	auto &keys_list_entry = variant.keys_data[result_index];
	uint32_t count = NumericCast<uint32_t>(iter.max);
	auto start_child_index = children_list_entry.offset + children_offset_data[result_index];
	WriteContainerData<WRITE_DATA>(result.variant, result_index, blob_offset_data[result_index], count,
	                               children_offset_data[result_index]);

	auto start_key_index = keys_offset_data[result_index];
	//! Reserve these indices for the object
	children_offset_data[result_index] += count;
	keys_offset_data[result_index] += count;

	//! Iterate over all the children in the Object
	yyjson_val *key, *val;
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto key_string = yyjson_get_str(key);
		uint32_t key_string_len = NumericCast<uint32_t>(unsafe_yyjson_get_len(key));

		if (WRITE_DATA) {
			auto str = string_t(key_string, key_string_len);
			auto keys_index = start_key_index++;
			auto dictionary_index = result.GetOrCreateIndex(str);

			//! Set the keys_index
			variant.keys_index_data[start_child_index] = keys_index;
			//! Set the values_index
			variant.values_index_data[start_child_index++] = values_offset_data[result_index];
			result.keys_selvec.set_index(keys_list_entry.offset + keys_index, dictionary_index);
		}

		val = yyjson_obj_iter_get_val(key);
		if (!ConvertJSON<WRITE_DATA, IGNORE_NULLS>(val, result, result_index, false)) {
			return false;
		}
	}
	return true;
}

template <bool WRITE_DATA>
static bool ConvertJSONPrimitive(yyjson_val *val, ToVariantGlobalResultData &result, idx_t result_index, bool is_root) {
	auto json_tag = unsafe_yyjson_get_tag(val);

	auto &variant = result.variant;
	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);
	auto blob_data = data_ptr_cast(variant.blob_data[result_index].GetDataWriteable());

	switch (json_tag) {
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_RAW | YYJSON_SUBTYPE_NONE: {
		WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
		                                 nullptr, 0, VariantLogicalType::VARCHAR);
		uint32_t length = NumericCast<uint32_t>(unsafe_yyjson_get_len(val));
		auto length_varint_size = GetVarintSize(length);
		if (WRITE_DATA) {
			auto str = unsafe_yyjson_get_str(val);
			auto str_blob_data = blob_data + blob_offset_data[result_index];
			VarintEncode(length, str_blob_data);
			memcpy(str_blob_data + length_varint_size, const_data_ptr_cast(str), length);
		}
		blob_offset_data[result_index] += length_varint_size + length;
		break;
	}
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE: {
		WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
		                                 nullptr, 0, VariantLogicalType::BOOL_TRUE);
		break;
	}
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE: {
		WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
		                                 nullptr, 0, VariantLogicalType::BOOL_FALSE);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT: {
		WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
		                                 nullptr, 0, VariantLogicalType::UINT64);
		if (WRITE_DATA) {
			auto value = unsafe_yyjson_get_uint(val);
			memcpy(blob_data + blob_offset_data[result_index], const_data_ptr_cast(&value), sizeof(uint64_t));
		}
		blob_offset_data[result_index] += sizeof(uint64_t);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT: {
		WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
		                                 nullptr, 0, VariantLogicalType::INT64);
		if (WRITE_DATA) {
			auto value = unsafe_yyjson_get_sint(val);
			memcpy(blob_data + blob_offset_data[result_index], const_data_ptr_cast(&value), sizeof(int64_t));
		}
		blob_offset_data[result_index] += sizeof(int64_t);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL: {
		WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
		                                 nullptr, 0, VariantLogicalType::DOUBLE);
		if (WRITE_DATA) {
			auto value = unsafe_yyjson_get_real(val);
			memcpy(blob_data + blob_offset_data[result_index], const_data_ptr_cast(&value), sizeof(double));
		}
		blob_offset_data[result_index] += sizeof(double);
		break;
	}
	default:
		throw InternalException("Unknown yyjson tag in ConvertJSON");
	}
	return true;
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertJSON(yyjson_val *val, ToVariantGlobalResultData &result, idx_t result_index, bool is_root) {
	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);

	if (unsafe_yyjson_is_null(val)) {
		if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
			                              nullptr, 0, is_root);
		}
		return true;
	}

	auto json_tag = unsafe_yyjson_get_tag(val);
	if (json_tag == (YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE)) {
		return ConvertJSONArray<WRITE_DATA, IGNORE_NULLS>(val, result, result_index, is_root);
	} else if (json_tag == (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE)) {
		return ConvertJSONObject<WRITE_DATA, IGNORE_NULLS>(val, result, result_index, is_root);
	} else {
		return ConvertJSONPrimitive<WRITE_DATA>(val, result, result_index, is_root);
	}
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertJSONToVariant(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count,
                          optional_ptr<const SelectionVector> selvec,
                          optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {
	D_ASSERT(source.vec.GetType().IsJSONType());

	auto &source_format = source.source_format;
	auto source_data = source_format.GetData<string_t>(source_format);

	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);

	auto &variant = result.variant;
	ReadJSONHolder json_holder;
	for (idx_t i = 0; i < count; i++) {
		auto source_index = source[i];
		auto result_index = selvec ? selvec->get_index(i) : i;

		if (!source_format.validity.RowIsValid(source_index)) {
			if (!IGNORE_NULLS) {
				HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
				                              values_index_selvec, i, is_root);
			}
			continue;
		}

		if (WRITE_DATA && values_index_selvec) {
			//! Write the 'values_index' for the parent of this column
			variant.values_index_data[values_index_selvec->get_index(i)] = values_offset_data[result_index];
		}

		auto &val = source_data[source_index];
		json_holder.doc =
		    yyjson_read(val.GetData(), val.GetSize(),
		                YYJSON_READ_ALLOW_INF_AND_NAN | YYJSON_READ_ALLOW_TRAILING_COMMAS | YYJSON_READ_BIGNUM_AS_RAW);
		if (!json_holder.doc) {
			if (!IGNORE_NULLS) {
				HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset_data[result_index],
				                              values_index_selvec, i, is_root);
			}
			continue;
		}
		auto *root = yyjson_doc_get_root(json_holder.doc);

		if (!ConvertJSON<WRITE_DATA, IGNORE_NULLS>(root, result, result_index, is_root)) {
			return false;
		}
		json_holder.Reset();
	}
	return true;
}

} // namespace variant
} // namespace duckdb
