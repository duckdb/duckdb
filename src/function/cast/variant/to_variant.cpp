#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/serializer/varint.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

//! TODO: all the conversion methods need an 'is_root' boolean
//! to figure out if the 'result' validity should be set to NULL or a VARIANT_NULL should be added

namespace {

struct OffsetData {
	static uint32_t *GetKeys(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[0]);
	}
	static uint32_t *GetChildren(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[1]);
	}
	static uint32_t *GetValues(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[2]);
	}
	static uint32_t *GetBlob(DataChunk &offsets) {
		return FlatVector::GetData<uint32_t>(offsets.data[3]);
	}
};

struct StringDictionary {
public:
	uint32_t AddString(Vector &list, string_t str) {
		auto it = dictionary.find(str);

		if (it != dictionary.end()) {
			return it->second;
		}
		auto dict_count = NumericCast<uint32_t>(dictionary.size());
		if (dict_count >= dictionary_capacity) {
			auto new_capacity = NextPowerOfTwo(dictionary_capacity + 1);
			ListVector::Reserve(list, new_capacity);
			dictionary_capacity = new_capacity;
		}
		auto &list_entry = ListVector::GetEntry(list);
		auto vec_data = FlatVector::GetData<string_t>(list_entry);
		vec_data[dict_count] = StringVector::AddStringOrBlob(list_entry, str);
		dictionary.emplace(vec_data[dict_count], dict_count);
		return dict_count;
	}

	idx_t Size() const {
		return dictionary.size();
	}

public:
	//! Ensure uniqueness of the dictionary entries
	string_map_t<uint32_t> dictionary;
	idx_t dictionary_capacity = STANDARD_VECTOR_SIZE;
};

struct VariantVectorData {
public:
	explicit VariantVectorData(Vector &variant)
	    : variant(variant), key_id_validity(FlatVector::Validity(VariantVector::GetChildrenKeyId(variant))),
	      keys(VariantVector::GetKeys(variant)) {
		blob_data = FlatVector::GetData<string_t>(VariantVector::GetData(variant));
		type_ids_data = FlatVector::GetData<uint8_t>(VariantVector::GetValuesTypeId(variant));
		byte_offset_data = FlatVector::GetData<uint32_t>(VariantVector::GetValuesByteOffset(variant));
		key_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenKeyId(variant));
		value_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenValueId(variant));
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
	uint32_t *key_id_data;
	uint32_t *value_id_data;
	ValidityMask &key_id_validity;

	//! values | children | keys
	list_entry_t *values_data;
	list_entry_t *children_data;
	list_entry_t *keys_data;

	Vector &keys;
};

struct EmptyConversionPayloadToVariant {};

//! enum
struct EnumConversionPayload {
public:
	EnumConversionPayload(const string_t *values, idx_t size) : values(values), size(size) {
	}

public:
	const string_t *values;
	idx_t size;
};

//! decimal
struct DecimalConversionPayloadToVariant {
public:
	DecimalConversionPayloadToVariant(idx_t width, idx_t scale) : width(width), scale(scale) {
	}

public:
	idx_t width;
	idx_t scale;
};

} // namespace

//===--------------------------------------------------------------------===//
// ANY -> VARIANT
//===--------------------------------------------------------------------===//

//! -------- Determine the 'type_id' for the Value --------

template <typename T, VariantLogicalType TYPE_ID>
static VariantLogicalType GetTypeId(T val) {
	return TYPE_ID;
}

template <>
VariantLogicalType GetTypeId<bool, VariantLogicalType::BOOL_TRUE>(bool val) {
	return val ? VariantLogicalType::BOOL_TRUE : VariantLogicalType::BOOL_FALSE;
}

//! -------- Write the 'value' data for the Value --------

template <typename T>
static void WriteData(data_ptr_t ptr, const T &val, vector<idx_t> &lengths, EmptyConversionPayloadToVariant &) {
	Store(val, ptr);
}
template <>
void WriteData(data_ptr_t ptr, const bool &val, vector<idx_t> &lengths, EmptyConversionPayloadToVariant &) {
	return;
}
template <>
void WriteData(data_ptr_t ptr, const string_t &val, vector<idx_t> &lengths, EmptyConversionPayloadToVariant &) {
	D_ASSERT(lengths.size() == 2);
	auto str_length = val.GetSize();
	VarintEncode(str_length, ptr);
	memcpy(ptr + lengths[0], val.GetData(), str_length);
}

//! decimal

template <typename T>
static void WriteData(data_ptr_t ptr, const T &val, vector<idx_t> &lengths,
                      DecimalConversionPayloadToVariant &payload) {
	throw InternalException("WriteData not implemented for this type");
}
template <>
void WriteData(data_ptr_t ptr, const int16_t &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(payload.width, ptr);
	VarintEncode(payload.scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}
template <>
void WriteData(data_ptr_t ptr, const int32_t &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(payload.width, ptr);
	VarintEncode(payload.scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}
template <>
void WriteData(data_ptr_t ptr, const int64_t &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(payload.width, ptr);
	VarintEncode(payload.scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}
template <>
void WriteData(data_ptr_t ptr, const hugeint_t &val, vector<idx_t> &lengths,
               DecimalConversionPayloadToVariant &payload) {
	D_ASSERT(lengths.size() == 3);
	VarintEncode(payload.width, ptr);
	VarintEncode(payload.scale, ptr + lengths[0]);
	Store(val, ptr + lengths[0] + lengths[1]);
}

//! enum

template <typename T>
static void WriteData(data_ptr_t ptr, const T &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	throw InternalException("WriteData not implemented for this Enum physical type");
}
template <>
void WriteData(data_ptr_t ptr, const uint8_t &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	EmptyConversionPayloadToVariant empty_payload;
	auto &str = payload.values[val];
	WriteData(ptr, str, lengths, empty_payload);
}
template <>
void WriteData(data_ptr_t ptr, const uint16_t &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	EmptyConversionPayloadToVariant empty_payload;
	auto &str = payload.values[val];
	WriteData(ptr, str, lengths, empty_payload);
}
template <>
void WriteData(data_ptr_t ptr, const uint32_t &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	EmptyConversionPayloadToVariant empty_payload;
	auto &str = payload.values[val];
	WriteData(ptr, str, lengths, empty_payload);
}

//! -------- Determine size of the 'value' data for the Value --------

template <typename T>
static void GetValueSize(const T &val, vector<idx_t> &lengths, EmptyConversionPayloadToVariant &) {
	lengths.push_back(sizeof(T));
}
template <>
void GetValueSize(const bool &val, vector<idx_t> &lengths, EmptyConversionPayloadToVariant &) {
}
template <>
void GetValueSize(const string_t &val, vector<idx_t> &lengths, EmptyConversionPayloadToVariant &) {
	auto value_size = val.GetSize();
	lengths.push_back(GetVarintSize(value_size));
	lengths.push_back(value_size);
}

//! decimal

template <typename T>
static void GetValueSize(const T &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	throw InternalException("GetValueSize not implemented for this Decimal physical type");
}
template <>
void GetValueSize(const int16_t &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	lengths.push_back(GetVarintSize(payload.width));
	lengths.push_back(GetVarintSize(payload.scale));
	lengths.push_back(sizeof(int16_t));
}
template <>
void GetValueSize(const int32_t &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	lengths.push_back(GetVarintSize(payload.width));
	lengths.push_back(GetVarintSize(payload.scale));
	lengths.push_back(sizeof(int32_t));
}
template <>
void GetValueSize(const int64_t &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	lengths.push_back(GetVarintSize(payload.width));
	lengths.push_back(GetVarintSize(payload.scale));
	lengths.push_back(sizeof(int64_t));
}
template <>
void GetValueSize(const hugeint_t &val, vector<idx_t> &lengths, DecimalConversionPayloadToVariant &payload) {
	lengths.push_back(GetVarintSize(payload.width));
	lengths.push_back(GetVarintSize(payload.scale));
	lengths.push_back(sizeof(hugeint_t));
}

//! enum

template <typename T>
static void GetValueSize(const T &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	throw InternalException("GetValueSize not implemented for this Enum physical type");
}
template <>
void GetValueSize(const uint8_t &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	EmptyConversionPayloadToVariant empty_payload;
	auto &str = payload.values[val];
	GetValueSize(str, lengths, empty_payload);
}
template <>
void GetValueSize(const uint16_t &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	EmptyConversionPayloadToVariant empty_payload;
	auto &str = payload.values[val];
	GetValueSize(str, lengths, empty_payload);
}
template <>
void GetValueSize(const uint32_t &val, vector<idx_t> &lengths, EnumConversionPayload &payload) {
	EmptyConversionPayloadToVariant empty_payload;
	auto &str = payload.values[val];
	GetValueSize(str, lengths, empty_payload);
}

template <bool WRITE_DATA>
static inline void WriteContainerData(VariantVectorData &result, idx_t result_index, uint32_t &blob_offset,
                                      idx_t length, idx_t children_offset) {
	const auto length_varint_size = GetVarintSize(length);
	const auto child_offset_varint_size =
	    NumericCast<uint32_t>(length_varint_size ? GetVarintSize(children_offset) : 0);

	if (WRITE_DATA) {
		auto &blob_value = result.blob_data[result_index];
		auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());
		VarintEncode(length, blob_value_data + blob_offset);
		if (length) {
			VarintEncode(children_offset, blob_value_data + blob_offset + length_varint_size);
		}
	}

	blob_offset += length_varint_size + child_offset_varint_size;
}

struct ContainerSelectionVectors {
public:
	explicit ContainerSelectionVectors(idx_t max_size)
	    : new_selection(0, max_size), non_null_selection(0, max_size), children_selection(0, max_size) {
	}

public:
	idx_t count = 0;
	//! Create a selection vector that maps to the right row in the 'result' for the child vector
	SelectionVector new_selection;
	//! Create a selection vector that maps to a subset (the rows where the parent isn't null) of rows of the child
	//! vector
	SelectionVector non_null_selection;
	//! Create a selection vector that maps to the children.value_id entry of the parent
	SelectionVector children_selection;
};

template <bool WRITE_DATA>
static inline void WriteArrayChildren(VariantVectorData &result, uint64_t children_offset,
                                      uint32_t &children_offset_data, const list_entry_t source_entry,
                                      idx_t result_index, ContainerSelectionVectors &sel) {
	idx_t children_index = children_offset + children_offset_data;
	for (idx_t child_idx = 0; child_idx < source_entry.length; child_idx++) {
		//! Set up the selection vector for the child of the list vector
		sel.new_selection.set_index(child_idx + sel.count, result_index);
		if (WRITE_DATA) {
			sel.children_selection.set_index(child_idx + sel.count, children_index + child_idx);
			result.key_id_validity.SetInvalid(children_index + child_idx);
		}
		sel.non_null_selection.set_index(sel.count + child_idx, source_entry.offset + child_idx);
	}
	children_offset_data += source_entry.length;
	sel.count += source_entry.length;
}

template <bool WRITE_DATA>
static inline void WriteVariantMetadata(VariantVectorData &result, idx_t result_index, uint32_t *values_offsets,
                                        uint32_t blob_offset, SelectionVector *value_ids_selvec, idx_t i,
                                        VariantLogicalType type_id) {

	auto &values_offset_data = values_offsets[result_index];
	if (WRITE_DATA) {
		auto &values_list_entry = result.values_data[result_index];
		auto values_offset = values_list_entry.offset;

		values_offset = values_list_entry.offset + values_offset_data;
		result.type_ids_data[values_offset] = static_cast<uint8_t>(type_id);
		result.byte_offset_data[values_offset] = blob_offset;
		if (value_ids_selvec) {
			result.value_id_data[value_ids_selvec->get_index(i)] = values_offset_data;
		}
	}
	values_offset_data++;
}

template <bool WRITE_DATA>
static inline void HandleVariantNull(VariantVectorData &result, idx_t result_index, uint32_t *values_offsets,
                                     uint32_t blob_offset, SelectionVector *value_ids_selvec, idx_t i,
                                     const bool is_root) {
	if (is_root) {
		if (WRITE_DATA) {
			FlatVector::SetNull(result.variant, result_index, true);
		}
		return;
	}
	WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offsets, blob_offset, value_ids_selvec, i,
	                                 VariantLogicalType::VARIANT_NULL);
}

//! -------- Convert primitive values to Variant --------

template <bool WRITE_DATA, bool IGNORE_NULLS, VariantLogicalType TYPE_ID, class T,
          class PAYLOAD_CLASS = EmptyConversionPayloadToVariant>
static bool ConvertPrimitiveToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                      SelectionVector *selvec, SelectionVector *value_ids_selvec,
                                      PAYLOAD_CLASS &payload, const bool is_root) {
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto &source_validity = source_format.validity;
	auto source_data = source_format.GetData<T>(source_format);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.sel->get_index(i);

		auto result_index = selvec ? selvec->get_index(i) : i;
		auto &blob_offset = blob_offset_data[result_index];

		vector<idx_t> lengths;

		if (TYPE_ID != VariantLogicalType::VARIANT_NULL && source_validity.RowIsValid(index)) {
			//! Write the value
			auto &val = source_data[index];
			GetValueSize<T>(val, lengths, payload);
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                                 GetTypeId<T, TYPE_ID>(val));
			if (WRITE_DATA) {
				auto &blob_value = result.blob_data[result_index];
				auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());
				WriteData<T>(blob_value_data + blob_offset, val, lengths, payload);
			}
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                              is_root);
		}

		for (auto &length : lengths) {
			blob_offset += length;
		}
	}
	return true;
}

//! fwd declare the ConvertToVariant function
template <bool WRITE_DATA, bool IGNORE_NULLS = false>
static bool ConvertToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                             SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                             SelectionVector *value_ids_selvec, const bool is_root);

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertArrayToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                  SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                  SelectionVector *value_ids_selvec, const bool is_root) {
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto children_offset_data = OffsetData::GetChildren(offsets);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto &source_validity = source_format.validity;

	auto array_type_size = ArrayType::GetSize(source.GetType());
	auto list_size = count * array_type_size;

	ContainerSelectionVectors sel(list_size);
	for (idx_t i = 0; i < count; i++) {
		const auto index = source_format.sel->get_index(i);
		const auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];
		auto &children_list_entry = result.children_data[result_index];

		list_entry_t source_entry;
		source_entry.offset = i * array_type_size;
		source_entry.length = array_type_size;

		if (source_validity.RowIsValid(index)) {
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                                 VariantLogicalType::ARRAY);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, source_entry.length,
			                               children_offset_data[result_index]);
			WriteArrayChildren<WRITE_DATA>(result, children_list_entry.offset, children_offset_data[result_index],
			                               source_entry, result_index, sel);
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                              is_root);
		}
	}

	//! Now write the child vector of the list
	auto &entry = ArrayVector::GetEntry(source);
	if (sel.count != list_size) {
		Vector sliced_entry(entry.GetType(), nullptr);
		sliced_entry.Dictionary(entry, list_size, sel.non_null_selection, sel.count);
		return ConvertToVariant<WRITE_DATA, false>(sliced_entry, result, offsets, sel.count, &sel.new_selection,
		                                           keys_selvec, dictionary, &sel.children_selection, false);
	} else {
		//! All rows are valid, no need to slice the child
		return ConvertToVariant<WRITE_DATA, false>(entry, result, offsets, sel.count, &sel.new_selection, keys_selvec,
		                                           dictionary, &sel.children_selection, false);
	}
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertListToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                 SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                 SelectionVector *value_ids_selvec, const bool is_root) {
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto children_offset_data = OffsetData::GetChildren(offsets);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto &source_validity = source_format.validity;
	auto source_data = source_format.GetData<list_entry_t>(source_format);

	auto list_size = ListVector::GetListSize(source);

	ContainerSelectionVectors sel(list_size);
	for (idx_t i = 0; i < count; i++) {
		const auto index = source_format.sel->get_index(i);
		const auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];
		auto &children_list_entry = result.children_data[result_index];

		if (source_validity.RowIsValid(index)) {
			auto &entry = source_data[index];
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                                 VariantLogicalType::ARRAY);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, entry.length,
			                               children_offset_data[result_index]);
			WriteArrayChildren<WRITE_DATA>(result, children_list_entry.offset, children_offset_data[result_index],
			                               entry, result_index, sel);
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                              is_root);
		}
	}
	//! Now write the child vector of the list (for all rows)
	auto &entry = ListVector::GetEntry(source);
	if (sel.count != list_size) {
		Vector sliced_entry(entry.GetType(), nullptr);
		sliced_entry.Dictionary(entry, list_size, sel.non_null_selection, sel.count);
		return ConvertToVariant<WRITE_DATA, false>(sliced_entry, result, offsets, sel.count, &sel.new_selection,
		                                           keys_selvec, dictionary, &sel.children_selection, false);
	} else {
		//! All rows are valid, no need to slice the child
		return ConvertToVariant<WRITE_DATA, false>(entry, result, offsets, sel.count, &sel.new_selection, keys_selvec,
		                                           dictionary, &sel.children_selection, false);
	}
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertStructToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                   SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                   SelectionVector *value_ids_selvec, const bool is_root) {
	auto keys_offset_data = OffsetData::GetKeys(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto children_offset_data = OffsetData::GetChildren(offsets);
	auto &type = source.GetType();

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto &source_validity = source_format.validity;

	auto &children = StructVector::GetEntries(source);

	//! Look up all the dictionary indices for the struct keys
	vector<uint32_t> dictionary_indices(children.size());
	if (WRITE_DATA) {
		auto &struct_children = StructType::GetChildTypes(type);
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &struct_child = struct_children[child_idx];
			string_t struct_child_str(struct_child.first.c_str(), NumericCast<uint32_t>(struct_child.first.size()));
			dictionary_indices[child_idx] = dictionary.AddString(result.keys, struct_child_str);
		}
	}

	ContainerSelectionVectors sel(count);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.sel->get_index(i);
		auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];
		auto &children_list_entry = result.children_data[result_index];
		auto &keys_list_entry = result.keys_data[result_index];

		if (source_validity.RowIsValid(index)) {
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                                 VariantLogicalType::OBJECT);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, children.size(),
			                               children_offset_data[result_index]);

			//! children
			if (WRITE_DATA) {
				idx_t children_index = children_list_entry.offset + children_offset_data[result_index];
				idx_t keys_offset = keys_list_entry.offset + keys_offset_data[result_index];
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					result.key_id_data[children_index + child_idx] =
					    NumericCast<uint32_t>(keys_offset_data[result_index] + child_idx);
					keys_selvec.set_index(keys_offset + child_idx, dictionary_indices[child_idx]);
				}
				//! Map from index of the child to the children.value_ids of the parent
				//! NOTE: this maps to the first index, below we are forwarding this for each child Vector we process.
				sel.children_selection.set_index(sel.count, children_index);
			}
			sel.non_null_selection.set_index(sel.count, i);
			sel.new_selection.set_index(sel.count, result_index);
			keys_offset_data[result_index] += children.size();
			children_offset_data[result_index] += children.size();
			sel.count++;
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                              is_root);
		}
	}

	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];

		if (sel.count != count) {
			//! Some of the STRUCT rows are NULL entirely, we have to filter these rows out of the children
			Vector new_child(child.GetType(), nullptr);
			new_child.Dictionary(child, count, sel.non_null_selection, sel.count);
			if (!ConvertToVariant<WRITE_DATA>(new_child, result, offsets, sel.count, &sel.new_selection, keys_selvec,
			                                  dictionary, &sel.children_selection, false)) {
				return false;
			}
		} else {
			if (!ConvertToVariant<WRITE_DATA>(child, result, offsets, sel.count, &sel.new_selection, keys_selvec,
			                                  dictionary, &sel.children_selection, false)) {
				return false;
			}
		}
		if (WRITE_DATA) {
			//! Now forward the selection to point to the next index in the children.value_ids
			for (idx_t i = 0; i < sel.count; i++) {
				sel.children_selection[i]++;
			}
		}
	}
	return true;
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertUnionToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                  SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                  SelectionVector *value_ids_selvec, const bool is_root) {
	auto &children = StructVector::GetEntries(source);

	UnifiedVectorFormat source_format;
	vector<UnifiedVectorFormat> member_formats(children.size());
	source.ToUnifiedFormat(count, source_format);
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		child.ToUnifiedFormat(count, member_formats[child_idx]);

		//! Convert all the children, ignore nulls, only write the non-null values
		//! UNION will have exactly 1 non-null value for each row
		if (!ConvertToVariant<WRITE_DATA, /*ignore_nulls = */ true>(child, result, offsets, count, selvec, keys_selvec,
		                                                            dictionary, value_ids_selvec, is_root)) {
			return false;
		}
	}

	if (IGNORE_NULLS) {
		return true;
	}

	//! For some reason we can have nulls in members, so we need this check
	//! So we are sure that we handled all nulls
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		bool is_null = true;
		for (idx_t child_idx = 1; child_idx < children.size() && is_null; child_idx++) {
			auto &child = *children[child_idx];
			if (child.GetType().id() == LogicalTypeId::SQLNULL) {
				continue;
			}
			auto &member_format = member_formats[child_idx];
			auto &member_validity = member_format.validity;
			is_null = !member_validity.RowIsValid(member_format.sel->get_index(i));
		}
		if (!is_null) {
			continue;
		}
		//! This row is NULL entirely
		auto result_index = selvec ? selvec->get_index(i) : i;
		auto &blob_offset = blob_offset_data[result_index];
		HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
		                              is_root);
	}
	return true;
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertVariantToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                    SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                    SelectionVector *value_ids_selvec, const bool is_root) {

	auto keys_offset_data = OffsetData::GetKeys(offsets);
	auto children_offset_data = OffsetData::GetChildren(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto blob_offset_data = OffsetData::GetBlob(offsets);

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source, count, source_format);

	//! source keys
	auto &source_keys = UnifiedVariantVector::GetKeys(source_format);
	auto source_keys_data = source_keys.GetData<list_entry_t>(source_keys);

	//! source keys entry
	auto &source_keys_entry = UnifiedVariantVector::GetKeysEntry(source_format);
	auto source_keys_entry_data = source_keys_entry.GetData<string_t>(source_keys_entry);

	//! source children
	auto &source_children = UnifiedVariantVector::GetChildren(source_format);
	auto source_children_data = source_children.GetData<list_entry_t>(source_children);

	//! source values
	auto &source_values = UnifiedVariantVector::GetValues(source_format);
	auto source_values_data = source_values.GetData<list_entry_t>(source_values);

	//! source data
	auto &source_data = UnifiedVariantVector::GetData(source_format);
	auto source_data_data = source_data.GetData<string_t>(source_data);

	//! source byte_offset
	auto &source_byte_offset = UnifiedVariantVector::GetValuesByteOffset(source_format);
	auto source_byte_offset_data = source_byte_offset.GetData<uint32_t>(source_byte_offset);

	//! source type_id
	auto &source_type_id = UnifiedVariantVector::GetValuesTypeId(source_format);
	auto source_type_id_data = source_type_id.GetData<uint8_t>(source_type_id);

	//! source key_id
	auto &source_key_id = UnifiedVariantVector::GetChildrenKeyId(source_format);
	auto source_key_id_data = source_key_id.GetData<uint32_t>(source_key_id);

	//! source value_id
	auto &source_value_id = UnifiedVariantVector::GetChildrenValueId(source_format);
	auto source_value_id_data = source_value_id.GetData<uint32_t>(source_value_id);

	auto &source_validity = source_format.unified.validity;

	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.unified.sel->get_index(i);
		auto result_index = selvec ? selvec->get_index(i) : i;

		auto &keys_list_entry = result.keys_data[result_index];
		auto &children_list_entry = result.children_data[result_index];
		auto blob_data = data_ptr_cast(result.blob_data[result_index].GetDataWriteable());

		auto &keys_offset = keys_offset_data[result_index];
		auto &children_offset = children_offset_data[result_index];
		auto &values_offset = values_offset_data[result_index];
		auto &blob_offset = blob_offset_data[result_index];

		idx_t keys_count = 0;
		idx_t blob_size = 0;
		if (!source_validity.RowIsValid(index)) {
			if (!IGNORE_NULLS) {
				HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec,
				                              i, is_root);
			}
			continue;
		}
		auto source_keys_list_entry = source_keys_data[index];
		auto source_children_list_entry = source_children_data[index];
		auto source_values_list_entry = source_values_data[index];
		auto source_blob_data = const_data_ptr_cast(source_data_data[index].GetData());

		D_ASSERT(source_values_list_entry.length);
		if (value_ids_selvec) {
			//! Write the value_id for the parent of this column
			result.value_id_data[value_ids_selvec->get_index(i)] = values_offset;
		}

		//! First write all children
		//! NOTE: this has to happen first because we use 'values_offset', which is increased when we write the values
		for (idx_t j = 0; j < source_children_list_entry.length; j++) {

			//! value_id
			if (WRITE_DATA) {
				auto source_value_id_index = source_value_id.sel->get_index(j + source_children_list_entry.offset);
				auto source_value_id_value = source_value_id_data[source_value_id_index];
				result.value_id_data[children_list_entry.offset + children_offset + j] =
				    values_offset + source_value_id_value;
			}

			//! key_id
			auto source_key_id_index = source_key_id.sel->get_index(j + source_children_list_entry.offset);
			if (source_key_id.validity.RowIsValid(source_key_id_index)) {
				if (WRITE_DATA) {
					auto source_key_id_value = source_key_id_data[source_key_id_index];

					//! Look up the existing key from 'source'
					auto source_key_entry_index =
					    source_keys_entry.sel->get_index(source_keys_list_entry.offset + source_key_id_value);
					auto &source_key_value = source_keys_entry_data[source_key_entry_index];

					//! Now write this key to the dictionary of the result
					auto dict_index = dictionary.AddString(result.keys, source_key_value);
					result.key_id_data[children_list_entry.offset + children_offset + j] =
					    NumericCast<uint32_t>(keys_offset + keys_count);
					keys_selvec.set_index(keys_list_entry.offset + keys_offset + keys_count, dict_index);
				}
				keys_count++;
			} else {
				if (WRITE_DATA) {
					result.key_id_validity.SetInvalid(children_list_entry.offset + children_offset + j);
				}
			}
		}

		//! Then write all values
		for (idx_t j = 0; j < source_values_list_entry.length; j++) {
			auto source_type_id_index = source_type_id.sel->get_index(j + source_values_list_entry.offset);
			auto source_type_id_value = static_cast<VariantLogicalType>(source_type_id_data[source_type_id_index]);

			auto source_byte_offset_index = source_byte_offset.sel->get_index(j + source_values_list_entry.offset);
			auto source_byte_offset_value = source_byte_offset_data[source_byte_offset_index];

			//! NOTE: we have to deserialize these in both passes
			//! because to figure out the size of the 'data' that is added by the VARIANT, we have to traverse the
			//! VARIANT solely because the 'child_index' stored in the OBJECT/ARRAY data could require more bits
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset + blob_size, nullptr,
			                                 0, source_type_id_value);
			switch (source_type_id_value) {
			case VariantLogicalType::ARRAY:
			case VariantLogicalType::OBJECT: {
				auto container_blob_data = source_blob_data + source_byte_offset_value;
				auto length = VarintDecode<uint32_t>(container_blob_data);
				if (WRITE_DATA) {
					VarintEncode(length, blob_data + blob_offset + blob_size);
				}
				blob_size += GetVarintSize(length);
				if (length) {
					auto child_index = VarintDecode<uint32_t>(container_blob_data);
					auto new_child_index = child_index + children_offset;
					if (WRITE_DATA) {
						VarintEncode(new_child_index, blob_data + blob_offset + blob_size);
					}
					blob_size += GetVarintSize(new_child_index);
				}
				break;
			}
			case VariantLogicalType::VARIANT_NULL:
			case VariantLogicalType::BOOL_FALSE:
			case VariantLogicalType::BOOL_TRUE:
				break;
			case VariantLogicalType::DECIMAL: {
				auto decimal_blob_data = source_blob_data + source_byte_offset_value;
				auto width = static_cast<uint8_t>(VarintDecode<uint32_t>(decimal_blob_data));
				auto width_varint_size = GetVarintSize(width);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, decimal_blob_data - width_varint_size,
					       width_varint_size);
				}
				blob_size += width_varint_size;
				auto scale = static_cast<uint8_t>(VarintDecode<uint32_t>(decimal_blob_data));
				auto scale_varint_size = GetVarintSize(scale);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, decimal_blob_data - scale_varint_size,
					       scale_varint_size);
				}
				blob_size += scale_varint_size;

				if (width > DecimalWidth<int64_t>::max) {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(hugeint_t));
					}
					blob_size += sizeof(hugeint_t);
				} else if (width > DecimalWidth<int32_t>::max) {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(int64_t));
					}
					blob_size += sizeof(int64_t);
				} else if (width > DecimalWidth<int16_t>::max) {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(int32_t));
					}
					blob_size += sizeof(int32_t);
				} else {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(int16_t));
					}
					blob_size += sizeof(int16_t);
				}
				break;
			}
			case VariantLogicalType::BITSTRING:
			case VariantLogicalType::BIGNUM:
			case VariantLogicalType::VARCHAR:
			case VariantLogicalType::BLOB: {
				auto str_blob_data = source_blob_data + source_byte_offset_value;
				auto str_length = VarintDecode<uint32_t>(str_blob_data);
				auto str_length_varint_size = GetVarintSize(str_length);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, str_blob_data - str_length_varint_size,
					       str_length_varint_size);
				}
				blob_size += str_length_varint_size;
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, str_blob_data, str_length);
				}
				blob_size += str_length;
				break;
			}
			case VariantLogicalType::INT8:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(int8_t));
				}
				blob_size += sizeof(int8_t);
				break;
			case VariantLogicalType::INT16:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(int16_t));
				}
				blob_size += sizeof(int16_t);
				break;
			case VariantLogicalType::INT32:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(int32_t));
				}
				blob_size += sizeof(int32_t);
				break;
			case VariantLogicalType::INT64:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(int64_t));
				}
				blob_size += sizeof(int64_t);
				break;
			case VariantLogicalType::INT128:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(hugeint_t));
				}
				blob_size += sizeof(hugeint_t);
				break;
			case VariantLogicalType::UINT8:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(uint8_t));
				}
				blob_size += sizeof(uint8_t);
				break;
			case VariantLogicalType::UINT16:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(uint16_t));
				}
				blob_size += sizeof(uint16_t);
				break;
			case VariantLogicalType::UINT32:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(uint32_t));
				}
				blob_size += sizeof(uint32_t);
				break;
			case VariantLogicalType::UINT64:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(uint64_t));
				}
				blob_size += sizeof(uint64_t);
				break;
			case VariantLogicalType::UINT128:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(uhugeint_t));
				}
				blob_size += sizeof(uhugeint_t);
				break;
			case VariantLogicalType::FLOAT:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(float));
				}
				blob_size += sizeof(float);
				break;
			case VariantLogicalType::DOUBLE:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(double));
				}
				blob_size += sizeof(double);
				break;
			case VariantLogicalType::UUID:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(hugeint_t));
				}
				blob_size += sizeof(hugeint_t);
				break;
			case VariantLogicalType::DATE:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(int32_t));
				}
				blob_size += sizeof(int32_t);
				break;
			case VariantLogicalType::TIME_MICROS:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(dtime_t));
				}
				blob_size += sizeof(dtime_t);
				break;
			case VariantLogicalType::TIME_NANOS:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(dtime_ns_t));
				}
				blob_size += sizeof(dtime_ns_t);
				break;
			case VariantLogicalType::TIMESTAMP_SEC:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(timestamp_sec_t));
				}
				blob_size += sizeof(timestamp_sec_t);
				break;
			case VariantLogicalType::TIMESTAMP_MILIS:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(timestamp_ms_t));
				}
				blob_size += sizeof(timestamp_ms_t);
				break;
			case VariantLogicalType::TIMESTAMP_MICROS:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(timestamp_t));
				}
				blob_size += sizeof(timestamp_t);
				break;
			case VariantLogicalType::TIMESTAMP_NANOS:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(timestamp_ns_t));
				}
				blob_size += sizeof(timestamp_ns_t);
				break;
			case VariantLogicalType::TIME_MICROS_TZ:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(dtime_tz_t));
				}
				blob_size += sizeof(dtime_tz_t);
				break;
			case VariantLogicalType::TIMESTAMP_MICROS_TZ:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(timestamp_tz_t));
				}
				blob_size += sizeof(timestamp_tz_t);
				break;
			case VariantLogicalType::INTERVAL:
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value,
					       sizeof(interval_t));
				}
				blob_size += sizeof(interval_t);
				break;
			default:
				throw InternalException("Unrecognized VariantLogicalType: %s",
				                        EnumUtil::ToString(source_type_id_value));
			}
		}

		keys_offset += keys_count;
		children_offset += source_children_list_entry.length;
		blob_offset += blob_size;
	}
	return true;
}

//! * @param source The Vector of arbitrary type to process
//! * @param result The result Vector to write the variant data to
//! * @param offsets The offsets to gather per row
//! * @param count The size of the source vector
//! * @param selvec The selection vector from i (< count) to the index in the result Vector
//! * @param keys_selvec The selection vector to populate with mapping from keys index -> dictionary index
//! * @param dictionary The dictionary to look up the dictionary index from
//! * @param value_ids_selvec The selection vector from i (< count) to the index in the children.value_ids selvec, to
//! populate the parent's children
template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                             SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                             SelectionVector *value_ids_selvec, const bool is_root) {
	auto &type = source.GetType();

	auto physical_type = type.InternalType();
	auto logical_type = type.id();
	if (type.IsNested()) {
		switch (logical_type) {
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST:
			return ConvertListToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                      dictionary, value_ids_selvec, is_root);
		case LogicalTypeId::ARRAY:
			return ConvertArrayToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                       dictionary, value_ids_selvec, is_root);
		case LogicalTypeId::STRUCT:
			return ConvertStructToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                        dictionary, value_ids_selvec, is_root);
		case LogicalTypeId::UNION:
			return ConvertUnionToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                       dictionary, value_ids_selvec, is_root);
		case LogicalTypeId::VARIANT:
			return ConvertVariantToVariant<WRITE_DATA, IGNORE_NULLS>(
			    source, result, offsets, count, selvec, keys_selvec, dictionary, value_ids_selvec, is_root);
		default:
			throw NotImplementedException("Can't convert nested type '%s'", EnumUtil::ToString(logical_type));
		};
	} else {
		EmptyConversionPayloadToVariant empty_payload;
		switch (type.id()) {
		case LogicalTypeId::SQLNULL:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARIANT_NULL, int32_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::BOOLEAN:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BOOL_TRUE, bool>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::TINYINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT8, int8_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::UTINYINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT8, uint8_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::SMALLINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT16, int16_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::USMALLINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT16, uint16_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::INTEGER:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT32, int32_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::UINTEGER:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT32, uint32_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::BIGINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT64, int64_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::UBIGINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT64, uint64_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::HUGEINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT128, hugeint_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::UHUGEINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT128, uhugeint_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::DATE:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DATE, date_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::TIME:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIME_MICROS, dtime_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::TIME_NS:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIME_NANOS, dtime_ns_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::TIMESTAMP:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_MICROS,
			                                 timestamp_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                              empty_payload, is_root);
		case LogicalTypeId::TIMESTAMP_SEC:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_SEC,
			                                 timestamp_sec_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                  empty_payload, is_root);
		case LogicalTypeId::TIMESTAMP_NS:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_NANOS,
			                                 timestamp_ns_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                 empty_payload, is_root);
		case LogicalTypeId::TIMESTAMP_MS:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_MILIS,
			                                 timestamp_ms_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                 empty_payload, is_root);
		case LogicalTypeId::TIME_TZ:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIME_MICROS_TZ, dtime_tz_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::TIMESTAMP_TZ:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_MICROS_TZ,
			                                 timestamp_tz_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                 empty_payload, is_root);
		case LogicalTypeId::UUID:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UUID, hugeint_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::FLOAT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::FLOAT, float>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::DOUBLE:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DOUBLE, double>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::DECIMAL: {
			uint8_t width;
			uint8_t scale;
			type.GetDecimalProperties(width, scale);
			DecimalConversionPayloadToVariant payload(width, scale);

			switch (physical_type) {
			case PhysicalType::INT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, int16_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload, is_root);
			case PhysicalType::INT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, int32_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload, is_root);
			case PhysicalType::INT64:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, int64_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload, is_root);
			case PhysicalType::INT128:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, hugeint_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload, is_root);
			default:
				throw NotImplementedException("Can't convert DECIMAL value of physical type: %s",
				                              EnumUtil::ToString(physical_type));
			};
		}
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::BLOB:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BLOB, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::INTERVAL:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INTERVAL, interval_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::ENUM: {
			auto &enum_values = EnumType::GetValuesInsertOrder(type);
			auto dict_size = EnumType::GetSize(type);
			D_ASSERT(enum_values.GetVectorType() == VectorType::FLAT_VECTOR);
			EnumConversionPayload payload(FlatVector::GetData<string_t>(enum_values), dict_size);
			switch (physical_type) {
			case PhysicalType::UINT8:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, uint8_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload, is_root);
			case PhysicalType::UINT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, uint16_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload, is_root);
			case PhysicalType::UINT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, uint32_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload, is_root);
			default:
				throw NotImplementedException("ENUM conversion for PhysicalType (%s) not supported",
				                              EnumUtil::ToString(physical_type));
			}
		}
		case LogicalTypeId::BIGNUM:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BIGNUM, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		case LogicalTypeId::BIT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BITSTRING, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload, is_root);
		default:
			throw NotImplementedException("Invalid LogicalType (%s) for ConvertToVariant",
			                              EnumUtil::ToString(logical_type));
		}
	}
	return true;
}

static void InitializeOffsets(DataChunk &offsets, idx_t count) {
	auto keys = OffsetData::GetKeys(offsets);
	auto children = OffsetData::GetChildren(offsets);
	auto values = OffsetData::GetValues(offsets);
	auto blob = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		keys[i] = 0;
		children[i] = 0;
		values[i] = 0;
		blob[i] = 0;
	}
}

static void InitializeVariants(DataChunk &offsets, Vector &result, SelectionVector &keys_selvec, idx_t &selvec_size,
                               StringDictionary &dictionary) {
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = ListVector::GetData(keys);

	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	idx_t keys_offset = 0;
	idx_t children_offset = 0;
	idx_t values_offset = 0;

	auto keys_sizes = OffsetData::GetKeys(offsets);
	auto children_sizes = OffsetData::GetChildren(offsets);
	auto values_sizes = OffsetData::GetValues(offsets);
	auto blob_sizes = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < offsets.size(); i++) {
		auto &keys_entry = keys_data[i];
		auto &children_entry = children_data[i];
		auto &values_entry = values_data[i];

		//! keys
		keys_entry.length = keys_sizes[i];
		keys_entry.offset = keys_offset;
		keys_offset += keys_entry.length;

		//! children
		children_entry.length = children_sizes[i];
		children_entry.offset = children_offset;
		children_offset += children_entry.length;

		//! values
		values_entry.length = values_sizes[i];
		values_entry.offset = values_offset;
		values_offset += values_entry.length;

		//! value
		blob_data[i] = StringVector::EmptyString(blob, blob_sizes[i]);
	}

	//! Reserve for the children of the lists
	ListVector::Reserve(keys, keys_offset);
	ListVector::Reserve(children, children_offset);
	ListVector::Reserve(values, values_offset);

	//! Set list sizes
	ListVector::SetListSize(keys, keys_offset);
	ListVector::SetListSize(children, children_offset);
	ListVector::SetListSize(values, values_offset);

	keys_selvec.Initialize(keys_offset);
	selvec_size = keys_offset;
}

static bool CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	DataChunk offsets;
	offsets.Initialize(Allocator::DefaultAllocator(),
	                   {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER},
	                   count);
	offsets.SetCardinality(count);
	auto &keys = VariantVector::GetKeys(result);

	//! Initialize the dictionary
	StringDictionary dictionary;
	SelectionVector keys_selvec;

	{
		VariantVectorData result_data(result);
		//! First pass - collect sizes/offsets
		InitializeOffsets(offsets, count);
		ConvertToVariant<false>(source, result_data, offsets, count, nullptr, keys_selvec, dictionary, nullptr, true);
	}

	//! This resizes the lists, invalidating the "GetData" results stored in VariantVectorData
	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size, dictionary);

	{
		VariantVectorData result_data(result);
		//! Second pass - actually construct the variants
		InitializeOffsets(offsets, count);
		ConvertToVariant<true>(source, result_data, offsets, count, nullptr, keys_selvec, dictionary, nullptr, true);
	}

	auto &keys_entry = ListVector::GetEntry(keys);
	VariantUtils::SortVariantKeys(keys_entry, dictionary.Size(), keys_selvec, keys_selvec_size);

	//! Finalize the 'keys'
	auto keys_entry_data = FlatVector::GetData<string_t>(keys_entry);
	for (idx_t i = 0; i < dictionary.Size(); i++) {
		keys_entry_data[i].SetSizeAndFinalize(static_cast<uint32_t>(keys_entry_data[i].GetSize()));
	}

	//! Finalize the 'data'
	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);
	for (idx_t i = 0; i < count; i++) {
		blob_data[i].SetSizeAndFinalize(static_cast<uint32_t>(blob_data[i].GetSize()));
	}

	keys_entry.Slice(keys_selvec, keys_selvec_size);
	keys_entry.Flatten(keys_selvec_size);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
	return true;
}

BoundCastInfo DefaultCasts::ImplicitToVariantCast(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {

	D_ASSERT(target.id() == LogicalTypeId::VARIANT);
	if (source.IsJSONType()) {
		return BoundCastInfo(VariantCasts::CastJSONToVARIANT);
	}
	return BoundCastInfo(CastToVARIANT);
}

} // namespace duckdb
