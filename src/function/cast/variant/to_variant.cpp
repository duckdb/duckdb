#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/serializer/varint.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

namespace {

struct OffsetData {
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
	void AddString(Vector &list, string_t str) {
		auto it = dictionary.find(str);

		if (it != dictionary.end()) {
			return;
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
	}

	idx_t Size() const {
		return dictionary.size();
	}

	uint32_t Find(string_t str) {
		auto it = dictionary.find(str);
		//! The dictionary was populated in the first pass, looked up in the second
		D_ASSERT(it != dictionary.end());
		return it->second;
	}

public:
	//! Ensure uniqueness of the dictionary entries
	string_map_t<uint32_t> dictionary;
	idx_t dictionary_capacity = STANDARD_VECTOR_SIZE;
};

struct VariantVectorData {
public:
	explicit VariantVectorData(Vector &variant)
	    : key_id_validity(FlatVector::Validity(VariantVector::GetChildrenKeyId(variant))) {
		blob_data = FlatVector::GetData<string_t>(VariantVector::GetData(variant));
		type_ids_data = FlatVector::GetData<uint8_t>(VariantVector::GetValuesTypeId(variant));
		byte_offset_data = FlatVector::GetData<uint32_t>(VariantVector::GetValuesByteOffset(variant));
		key_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenKeyId(variant));
		value_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenValueId(variant));
		values_data = FlatVector::GetData<list_entry_t>(VariantVector::GetValues(variant));
		children_data = FlatVector::GetData<list_entry_t>(VariantVector::GetChildren(variant));
		keys_data = FlatVector::GetData<list_entry_t>(VariantVector::GetKeys(variant));
	}

public:
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
static inline void WriteContainerChildren(VariantVectorData &result, uint64_t children_offset,
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
static inline void WriteVariantMetadata(VariantVectorData &result, uint64_t values_offset, uint32_t &values_offset_data,
                                        uint32_t blob_offset, SelectionVector *value_ids_selvec, idx_t i,
                                        VariantLogicalType type_id) {
	if (WRITE_DATA) {
		values_offset += values_offset_data;
		result.type_ids_data[values_offset] = static_cast<uint8_t>(type_id);
		result.byte_offset_data[values_offset] = blob_offset;
		if (value_ids_selvec) {
			result.value_id_data[value_ids_selvec->get_index(i)] = values_offset_data;
		}
	}
	values_offset_data++;
}

template <bool WRITE_DATA>
static inline void HandleVariantNull(VariantVectorData &result, uint64_t values_offset, uint32_t &values_offset_data,
                                     uint32_t blob_offset, SelectionVector *value_ids_selvec, idx_t i) {
	WriteVariantMetadata<WRITE_DATA>(result, values_offset, values_offset_data, blob_offset, value_ids_selvec, i,
	                                 VariantLogicalType::VARIANT_NULL);
}

//! -------- Convert primitive values to Variant --------

template <bool WRITE_DATA, bool IGNORE_NULLS, VariantLogicalType TYPE_ID, class T,
          class PAYLOAD_CLASS = EmptyConversionPayloadToVariant>
static bool ConvertPrimitiveToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                      SelectionVector *selvec, SelectionVector *value_ids_selvec,
                                      PAYLOAD_CLASS &payload) {
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
		auto &values_list_entry = result.values_data[result_index];

		vector<idx_t> lengths;

		if (TYPE_ID != VariantLogicalType::VARIANT_NULL && source_validity.RowIsValid(index)) {
			//! Write the value
			auto &val = source_data[index];
			GetValueSize<T>(val, lengths, payload);
			WriteVariantMetadata<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                                 blob_offset, value_ids_selvec, i, GetTypeId<T, TYPE_ID>(val));
			if (WRITE_DATA) {
				auto &blob_value = result.blob_data[result_index];
				auto blob_value_data = data_ptr_cast(blob_value.GetDataWriteable());
				WriteData<T>(blob_value_data + blob_offset, val, lengths, payload);
			}
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                              blob_offset, value_ids_selvec, i);
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
                             SelectionVector *value_ids_selvec);

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertArrayToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                  SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                  SelectionVector *value_ids_selvec) {
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
		auto &values_list_entry = result.values_data[result_index];
		auto &children_list_entry = result.children_data[result_index];

		list_entry_t source_entry;
		source_entry.offset = i * array_type_size;
		source_entry.length = array_type_size;

		if (source_validity.RowIsValid(index)) {
			WriteVariantMetadata<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                                 blob_offset, value_ids_selvec, i, VariantLogicalType::ARRAY);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, source_entry.length,
			                               children_offset_data[result_index]);
			WriteContainerChildren<WRITE_DATA>(result, children_list_entry.offset, children_offset_data[result_index],
			                                   source_entry, result_index, sel);
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                              blob_offset, value_ids_selvec, i);
		}
	}

	//! Now write the child vector of the list
	auto &entry = ArrayVector::GetEntry(source);
	if (sel.count != list_size) {
		Vector sliced_entry(entry.GetType(), nullptr);
		sliced_entry.Dictionary(entry, list_size, sel.non_null_selection, sel.count);
		return ConvertToVariant<WRITE_DATA, false>(sliced_entry, result, offsets, sel.count, &sel.new_selection,
		                                           keys_selvec, dictionary, &sel.children_selection);
	} else {
		//! All rows are valid, no need to slice the child
		return ConvertToVariant<WRITE_DATA, false>(entry, result, offsets, sel.count, &sel.new_selection, keys_selvec,
		                                           dictionary, &sel.children_selection);
	}
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertListToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                 SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                 SelectionVector *value_ids_selvec) {
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
		auto &values_list_entry = result.values_data[result_index];
		auto &children_list_entry = result.children_data[result_index];

		if (source_validity.RowIsValid(index)) {
			auto &entry = source_data[index];
			WriteVariantMetadata<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                                 blob_offset, value_ids_selvec, i, VariantLogicalType::ARRAY);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, entry.length,
			                               children_offset_data[result_index]);
			WriteContainerChildren<WRITE_DATA>(result, children_list_entry.offset, children_offset_data[result_index],
			                                   entry, result_index, sel);
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                              blob_offset, value_ids_selvec, i);
		}
	}
	//! Now write the child vector of the list (for all rows)
	auto &entry = ListVector::GetEntry(source);
	if (sel.count != list_size) {
		Vector sliced_entry(entry.GetType(), nullptr);
		sliced_entry.Dictionary(entry, list_size, sel.non_null_selection, sel.count);
		return ConvertToVariant<WRITE_DATA, false>(sliced_entry, result, offsets, sel.count, &sel.new_selection,
		                                           keys_selvec, dictionary, &sel.children_selection);
	} else {
		//! All rows are valid, no need to slice the child
		return ConvertToVariant<WRITE_DATA, false>(entry, result, offsets, sel.count, &sel.new_selection, keys_selvec,
		                                           dictionary, &sel.children_selection);
	}
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
static bool ConvertStructToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                                   SelectionVector *selvec, SelectionVector &keys_selvec, StringDictionary &dictionary,
                                   SelectionVector *value_ids_selvec) {
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
			dictionary_indices[child_idx] = dictionary.Find(struct_child_str);
		}
	}

	ContainerSelectionVectors sel(count);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.sel->get_index(i);
		auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];
		auto &values_list_entry = result.values_data[result_index];
		auto &children_list_entry = result.children_data[result_index];

		if (source_validity.RowIsValid(index)) {
			WriteVariantMetadata<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                                 blob_offset, value_ids_selvec, i, VariantLogicalType::OBJECT);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, children.size(),
			                               children_offset_data[result_index]);

			//! children
			if (WRITE_DATA) {
				idx_t children_index = children_list_entry.offset + children_offset_data[result_index];
				idx_t keys_offset = dictionary.Size() * result_index;
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					result.key_id_data[children_index + child_idx] = dictionary_indices[child_idx];
					keys_selvec.set_index(keys_offset + dictionary_indices[child_idx], dictionary_indices[child_idx]);
				}
				//! Map from index of the child to the children.value_ids of the parent
				//! NOTE: this maps to the first index, below we are forwarding this for each child Vector we process.
				sel.children_selection.set_index(sel.count, children_index);
			}
			sel.non_null_selection.set_index(sel.count, i);
			sel.new_selection.set_index(sel.count, result_index);
			children_offset_data[result_index] += children.size();
			sel.count++;
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index],
			                              blob_offset, value_ids_selvec, i);
		}
	}

	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];

		if (sel.count != count) {
			//! Some of the STRUCT rows are NULL entirely, we have to filter these rows out of the children
			Vector new_child(child.GetType(), nullptr);
			new_child.Dictionary(child, count, sel.non_null_selection, sel.count);
			if (!ConvertToVariant<WRITE_DATA>(new_child, result, offsets, sel.count, &sel.new_selection, keys_selvec,
			                                  dictionary, &sel.children_selection)) {
				return false;
			}
		} else {
			if (!ConvertToVariant<WRITE_DATA>(child, result, offsets, sel.count, &sel.new_selection, keys_selvec,
			                                  dictionary, &sel.children_selection)) {
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
                                  SelectionVector *value_ids_selvec) {
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
		                                                            dictionary, value_ids_selvec)) {
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
		auto &values_list_entry = result.values_data[result_index];

		HandleVariantNull<WRITE_DATA>(result, values_list_entry.offset, values_offset_data[result_index], blob_offset,
		                              value_ids_selvec, i);
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
                             SelectionVector *value_ids_selvec) {
	auto &type = source.GetType();

	auto physical_type = type.InternalType();
	auto logical_type = type.id();
	if (type.IsNested()) {
		switch (logical_type) {
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST:
			return ConvertListToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                      dictionary, value_ids_selvec);
		case LogicalTypeId::ARRAY:
			return ConvertArrayToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                       dictionary, value_ids_selvec);
		case LogicalTypeId::STRUCT:
			return ConvertStructToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                        dictionary, value_ids_selvec);
		case LogicalTypeId::UNION:
			return ConvertUnionToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                       dictionary, value_ids_selvec);
		default:
			throw NotImplementedException("Can't convert nested type '%s'", EnumUtil::ToString(logical_type));
		};
	} else {
		EmptyConversionPayloadToVariant empty_payload;
		switch (type.id()) {
		case LogicalTypeId::SQLNULL:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARIANT_NULL, int32_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::BOOLEAN:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BOOL_TRUE, bool>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::TINYINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT8, int8_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::UTINYINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT8, uint8_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::SMALLINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT16, int16_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::USMALLINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT16, uint16_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::INTEGER:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT32, int32_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::UINTEGER:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT32, uint32_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::BIGINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT64, int64_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::UBIGINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT64, uint64_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::HUGEINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INT128, hugeint_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::UHUGEINT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UINT128, uhugeint_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::DATE:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DATE, date_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::TIME:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIME_MICROS, dtime_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::TIME_NS:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIME_NANOS, dtime_ns_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::TIMESTAMP:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_MICROS,
			                                 timestamp_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                              empty_payload);
		case LogicalTypeId::TIMESTAMP_SEC:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_SEC,
			                                 timestamp_sec_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                  empty_payload);
		case LogicalTypeId::TIMESTAMP_NS:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_NANOS,
			                                 timestamp_ns_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                 empty_payload);
		case LogicalTypeId::TIMESTAMP_MS:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_MILIS,
			                                 timestamp_ms_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                 empty_payload);
		case LogicalTypeId::TIME_TZ:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIME_MICROS_TZ, dtime_tz_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::TIMESTAMP_TZ:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::TIMESTAMP_MICROS_TZ,
			                                 timestamp_tz_t>(source, result, offsets, count, selvec, value_ids_selvec,
			                                                 empty_payload);
		case LogicalTypeId::UUID:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::UUID, hugeint_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::FLOAT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::FLOAT, float>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::DOUBLE:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DOUBLE, double>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::DECIMAL: {
			uint8_t width;
			uint8_t scale;
			type.GetDecimalProperties(width, scale);
			DecimalConversionPayloadToVariant payload(width, scale);

			switch (physical_type) {
			case PhysicalType::INT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, int16_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload);
			case PhysicalType::INT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, int32_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload);
			case PhysicalType::INT64:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, int64_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload);
			case PhysicalType::INT128:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::DECIMAL, hugeint_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload);
			default:
				throw NotImplementedException("Can't convert DECIMAL value of physical type: %s",
				                              EnumUtil::ToString(physical_type));
			};
		}
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::BLOB:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BLOB, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::INTERVAL:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::INTERVAL, interval_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::ENUM: {
			auto &enum_values = EnumType::GetValuesInsertOrder(type);
			auto dict_size = EnumType::GetSize(type);
			D_ASSERT(enum_values.GetVectorType() == VectorType::FLAT_VECTOR);
			EnumConversionPayload payload(FlatVector::GetData<string_t>(enum_values), dict_size);
			switch (physical_type) {
			case PhysicalType::UINT8:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, uint8_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload);
			case PhysicalType::UINT16:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, uint16_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload);
			case PhysicalType::UINT32:
				return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::VARCHAR, uint32_t>(
				    source, result, offsets, count, selvec, value_ids_selvec, payload);
			default:
				throw NotImplementedException("ENUM conversion for PhysicalType (%s) not supported",
				                              EnumUtil::ToString(physical_type));
			}
		}
		case LogicalTypeId::BIGNUM:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BIGNUM, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		case LogicalTypeId::BIT:
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS, VariantLogicalType::BITSTRING, string_t>(
			    source, result, offsets, count, selvec, value_ids_selvec, empty_payload);
		default:
			throw NotImplementedException("Invalid LogicalType (%s) for ConvertToVariant",
			                              EnumUtil::ToString(logical_type));
		}
	}
	return true;
}

static void InitializeOffsets(DataChunk &offsets, idx_t count) {
	auto children = OffsetData::GetChildren(offsets);
	auto values = OffsetData::GetValues(offsets);
	auto blob = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
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

	idx_t children_offset = 0;
	idx_t values_offset = 0;

	auto dictionary_size = dictionary.Size();

	auto children_sizes = OffsetData::GetChildren(offsets);
	auto values_sizes = OffsetData::GetValues(offsets);
	auto blob_sizes = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < offsets.size(); i++) {
		auto &keys_entry = keys_data[i];
		auto &children_entry = children_data[i];
		auto &values_entry = values_data[i];

		//! keys
		keys_entry.length = dictionary_size;
		keys_entry.offset = dictionary_size * i;

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
	auto keys_list_size = dictionary_size * offsets.size();

	//! Reserve for the children of the lists
	ListVector::Reserve(keys, keys_list_size);
	ListVector::Reserve(children, children_offset);
	ListVector::Reserve(values, values_offset);

	//! Set list sizes
	ListVector::SetListSize(keys, keys_list_size);
	ListVector::SetListSize(children, children_offset);
	ListVector::SetListSize(values, values_offset);

	keys_selvec.Initialize(keys_list_size);
	//! NOTE: we initialize these to 0 because some rows will not set them, if the row is NULL
	//! To simplify the implementation we just allocate 'dictionary.Size()' keys for each row
	for (idx_t i = 0; i < keys_list_size; i++) {
		keys_selvec.set_index(i, 0);
	}
	selvec_size = keys_list_size;
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
	{
		TypeVisitor::Contains(source.GetType(), [&dictionary, &keys](const LogicalType &type) {
			if (type.id() == LogicalTypeId::STRUCT) {
				auto &children = StructType::GetChildTypes(type);
				for (auto &child : children) {
					string_t child_name_str(child.first.c_str(), NumericCast<uint32_t>(child.first.size()));
					dictionary.AddString(keys, child_name_str);
				}
			} else if (type.id() == LogicalTypeId::MAP) {
				dictionary.AddString(keys, string_t("key", 3));
				dictionary.AddString(keys, string_t("value", 5));
			}
			return false;
		});
	}
	SelectionVector keys_selvec;

	{
		VariantVectorData result_data(result);
		//! First pass - collect sizes/offsets
		InitializeOffsets(offsets, count);
		ConvertToVariant<false>(source, result_data, offsets, count, nullptr, keys_selvec, dictionary, nullptr);
	}

	//! This resizes the lists, invalidating the "GetData" results stored in VariantVectorData
	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size, dictionary);

	{
		VariantVectorData result_data(result);
		//! Second pass - actually construct the variants
		InitializeOffsets(offsets, count);
		ConvertToVariant<true>(source, result_data, offsets, count, nullptr, keys_selvec, dictionary, nullptr);
	}

	auto &keys_entry = ListVector::GetEntry(keys);
	VariantUtils::SortVariantKeys(keys_entry, dictionary.Size(), keys_selvec, keys_selvec_size);

	keys_entry.Slice(keys_selvec, keys_selvec_size);
	keys_entry.Flatten(keys_selvec_size);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
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
