#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

//! ------------ JSON -> Variant ------------

struct VariantConversionState {
public:
	explicit VariantConversionState() {
	}

public:
	//! Reset for every row
	void Reset() {
		stream.Rewind();

		//! Update the total count
		value_count += row_value_count;
		children_count += row_children_count;
		keys_count += key_ids.size();

		//! Reset the row-local counts
		row_value_count = 0;
		row_children_count = 0;
		key_ids.clear();
	}

public:
	uint32_t AddString(Vector &vec, string_t str) {
		auto it = dictionary.find(str);
		auto local_it = key_ids.find(str);

		uint32_t dict_index;
		if (it == dictionary.end()) {
			//! Key does not exist in the global dictionary yet
			auto vec_data = FlatVector::GetData<string_t>(vec);
			auto dict_count = NumericCast<uint32_t>(dictionary.size());
			if (dict_count >= dictionary_capacity) {
				auto new_capacity = NextPowerOfTwo(dictionary_capacity + 1);
				vec.Resize(dictionary_capacity, new_capacity);
				dictionary_capacity = new_capacity;
			}
			vec_data[dict_count] = StringVector::AddStringOrBlob(vec, str);
			it = dictionary.emplace(vec_data[dict_count], dict_count).first;
		}
		dict_index = it->second;

		if (local_it == key_ids.end()) {
			auto vec_data = FlatVector::GetData<string_t>(vec);
			local_it = key_ids.emplace(vec_data[dict_index], NumericCast<uint32_t>(key_ids.size())).first;
		}

		auto keys_idx = keys_count + local_it->second;
		if (!sel_vec_capacity || keys_idx >= sel_vec_capacity) {
			//! Reinitialize the selection vector
			auto new_capacity = !sel_vec_capacity ? STANDARD_VECTOR_SIZE : NextPowerOfTwo(sel_vec_capacity + 1);
			auto new_selection_data = make_shared_ptr<SelectionData>(new_capacity);
			if (sel_vec.data() && sel_vec_capacity) {
				memcpy(new_selection_data->owned_data.get(), sel_vec.data(), sizeof(sel_t) * sel_vec_capacity);
			}
			sel_vec.Initialize(std::move(new_selection_data));
			sel_vec_capacity = new_capacity;
		}
		sel_vec.set_index(keys_idx, dict_index);
		return local_it->second;
	}

public:
	uint32_t children_count = 0;
	uint32_t keys_count = 0;
	uint32_t key_id = 0;
	uint32_t value_count = 0;
	//! Record the relationship between index in the 'keys' (child) and the index in the dictionary
	SelectionVector sel_vec;
	idx_t sel_vec_capacity = 0;
	//! Ensure uniqueness of the dictionary entries
	string_map_t<uint32_t> dictionary;
	idx_t dictionary_capacity = STANDARD_VECTOR_SIZE;

public:
	//! State for the current row

	//! amount of values in the row
	uint32_t row_value_count = 0;
	//! amount of children in the row
	uint32_t row_children_count = 0;
	//! stream used to write the binary data
	MemoryStream stream;
	//! For the current row, the (duplicate eliminated) key ids
	string_map_t<uint32_t> key_ids;
};

} // namespace

static optional_idx ConvertJSON(yyjson_val *val, Vector &result, VariantConversionState &state);

static bool ConvertJSONArray(yyjson_val *arr, Vector &result, VariantConversionState &state) {
	auto &children = VariantVector::GetChildren(result);
	auto &value_ids = VariantVector::GetChildrenValueId(result);
	auto &key_ids = VariantVector::GetChildrenKeyId(result);

	yyjson_arr_iter iter;
	yyjson_arr_iter_init(arr, &iter);

	//! Write the 'value' blob for the ARRAY
	uint32_t count = NumericCast<uint32_t>(iter.max);
	auto start_child_index = state.children_count + state.row_children_count;
	VarintEncode(count, state.stream);
	if (count) {
		VarintEncode(state.row_children_count, state.stream);
	}

	//! Reserve these indices for the array
	state.row_children_count += count;

	auto &values = VariantVector::GetValues(result);
	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	ListVector::Reserve(children, state.children_count + state.row_children_count);

	auto value_ids_data = FlatVector::GetData<uint32_t>(value_ids);
	auto &key_ids_validity = FlatVector::Validity(key_ids);
	//! Iterate over all the children in the Array
	while (yyjson_arr_iter_has_next(&iter)) {
		auto val = yyjson_arr_iter_next(&iter);

		auto child_index = state.row_value_count;
		auto res = ConvertJSON(val, result, state);
		if (!res.IsValid()) {
			return false;
		}

		//! Set the child index
		key_ids_validity.SetInvalid(start_child_index);
		value_ids_data[start_child_index++] = child_index;
	}
	return true;
}

static bool ConvertJSONObject(yyjson_val *obj, Vector &result, VariantConversionState &state) {
	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);

	auto &children = VariantVector::GetChildren(result);

	auto &key_ids = VariantVector::GetChildrenKeyId(result);
	auto &value_ids = VariantVector::GetChildrenValueId(result);

	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);

	//! Write the 'value' blob for the OBJECT
	uint32_t count = NumericCast<uint32_t>(iter.max);
	auto start_child_index = state.children_count + state.row_children_count;

	VarintEncode(count, state.stream);
	if (count) {
		VarintEncode(state.row_children_count, state.stream);
	}

	//! Reserve these indices for the object
	state.row_children_count += count;

	auto &values = VariantVector::GetValues(result);
	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	ListVector::Reserve(children, state.children_count + state.row_children_count);

	auto key_ids_data = FlatVector::GetData<uint32_t>(key_ids);
	auto value_ids_data = FlatVector::GetData<uint32_t>(value_ids);
	//! Iterate over all the children in the Object
	yyjson_val *key, *val;
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto key_string = yyjson_get_str(key);
		uint32_t key_string_len = NumericCast<uint32_t>(unsafe_yyjson_get_len(key));

		val = yyjson_obj_iter_get_val(key);
		auto child_index = state.row_value_count;
		auto res = ConvertJSON(val, result, state);
		if (!res.IsValid()) {
			return false;
		}

		auto str = string_t(key_string, key_string_len);
		auto key_id = state.AddString(keys_entry, str);

		//! Set the key_id
		key_ids_data[start_child_index] = key_id;
		//! Set the value_id
		value_ids_data[start_child_index] = child_index;
		start_child_index++;
	}
	return true;
}

static optional_idx ConvertJSON(yyjson_val *val, Vector &result, VariantConversionState &state) {
	auto &type_ids = VariantVector::GetValuesTypeId(result);
	auto &byte_offsets = VariantVector::GetValuesByteOffset(result);

	auto type_ids_data = FlatVector::GetData<uint8_t>(type_ids);
	auto byte_offsets_data = FlatVector::GetData<uint32_t>(byte_offsets);

	auto index = state.value_count + state.row_value_count++;
	byte_offsets_data[index] = NumericCast<uint32_t>(state.stream.GetPosition());

	if (unsafe_yyjson_is_null(val)) {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL);
		return index;
	}

	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_RAW | YYJSON_SUBTYPE_NONE: {
		auto str = unsafe_yyjson_get_str(val);
		uint32_t length = NumericCast<uint32_t>(unsafe_yyjson_get_len(val));
		VarintEncode(length, state.stream);
		state.stream.WriteData(const_data_ptr_cast(str), length);
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::VARCHAR);
		break;
	}
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::ARRAY);
		if (!ConvertJSONArray(val, result, state)) {
			return optional_idx();
		}
		break;
	}
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::OBJECT);
		if (!ConvertJSONObject(val, result, state)) {
			return optional_idx();
		}
		break;
	}
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::BOOL_TRUE);
		break;
	}
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE: {
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::BOOL_FALSE);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT: {
		auto value = unsafe_yyjson_get_uint(val);
		state.stream.WriteData(const_data_ptr_cast(&value), sizeof(uint64_t));
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::UINT64);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT: {
		auto value = unsafe_yyjson_get_sint(val);
		state.stream.WriteData(const_data_ptr_cast(&value), sizeof(int64_t));
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::INT64);
		break;
	}
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL: {
		auto value = unsafe_yyjson_get_real(val);
		state.stream.WriteData(const_data_ptr_cast(&value), sizeof(double));
		type_ids_data[index] = static_cast<uint8_t>(VariantLogicalType::DOUBLE);
		break;
	}
	default:
		throw InternalException("Unknown yyjson tag in ConvertJSON");
	}
	return index;
}

bool VariantCasts::CastJSONToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto source_data = source_format.GetData<string_t>(source_format);
	VariantConversionState state;

	//! keys
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = FlatVector::GetData<list_entry_t>(keys);
	ListVector::SetListSize(keys, 0);

	//! children
	auto &children = VariantVector::GetChildren(result);
	auto children_data = FlatVector::GetData<list_entry_t>(children);
	ListVector::SetListSize(children, 0);

	//! values
	auto &values = VariantVector::GetValues(result);
	auto values_data = FlatVector::GetData<list_entry_t>(values);
	ListVector::SetListSize(values, 0);

	auto &value = VariantVector::GetData(result);
	auto value_data = FlatVector::GetData<string_t>(value);

	ListVector::Reserve(values, state.value_count + state.row_value_count + count);
	for (idx_t i = 0; i < count; i++) {
		auto source_index = source_format.sel->get_index(i);
		auto &val = source_data[source_index];

		auto *doc = yyjson_read(val.GetData(), val.GetSize(), 0);
		auto *root = yyjson_doc_get_root(doc);

		//! keys
		auto &keys_list_entry = keys_data[i];
		keys_list_entry.offset = ListVector::GetListSize(keys);

		//! children
		auto &children_list_entry = children_data[i];
		children_list_entry.offset = ListVector::GetListSize(children);

		//! values
		auto &values_list_entry = values_data[i];
		values_list_entry.offset = ListVector::GetListSize(values);

		auto value_index = ConvertJSON(root, result, state);
		if (!value_index.IsValid()) {
			return false;
		}

		//! keys
		keys_list_entry.length = state.key_ids.size();
		ListVector::SetListSize(keys, keys_list_entry.offset + keys_list_entry.length);

		//! children
		children_list_entry.length = state.row_children_count;
		ListVector::SetListSize(children, children_list_entry.offset + children_list_entry.length);

		//! values
		values_list_entry.length = state.row_value_count;
		ListVector::SetListSize(values, values_list_entry.offset + values_list_entry.length);

		//! value
		auto size = state.stream.GetPosition();
		auto stream_data = state.stream.GetData();
		value_data[i] = StringVector::AddStringOrBlob(value, reinterpret_cast<const char *>(stream_data), size);
		state.Reset();
	}

	auto &dictionary = ListVector::GetEntry(keys);
	auto dictionary_size = state.dictionary.size();

	auto &sel = state.sel_vec;
	auto sel_size = state.keys_count;

	VariantUtils::SortVariantKeys(dictionary, dictionary_size, sel, sel_size);
	dictionary.Slice(state.sel_vec, state.keys_count);
	dictionary.Flatten(state.keys_count);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

} // namespace duckdb
