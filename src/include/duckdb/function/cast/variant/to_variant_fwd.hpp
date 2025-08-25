#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/owning_string_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {
namespace variant {

struct OffsetData {
public:
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

template <bool WRITE_DATA>
void WriteContainerData(VariantVectorData &result, idx_t result_index, uint32_t &blob_offset, idx_t length,
                        idx_t children_offset) {
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
void WriteArrayChildren(VariantVectorData &result, uint64_t children_offset, uint32_t &children_offset_data,
                        const list_entry_t source_entry, idx_t result_index, ContainerSelectionVectors &sel) {
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
void WriteVariantMetadata(VariantVectorData &result, idx_t result_index, uint32_t *values_offsets, uint32_t blob_offset,
                          optional_ptr<SelectionVector> value_ids_selvec, idx_t i, VariantLogicalType type_id) {

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
void HandleVariantNull(VariantVectorData &result, idx_t result_index, uint32_t *values_offsets, uint32_t blob_offset,
                       optional_ptr<SelectionVector> value_ids_selvec, idx_t i, const bool is_root) {
	if (is_root) {
		if (WRITE_DATA) {
			FlatVector::SetNull(result.variant, result_index, true);
		}
		return;
	}
	WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offsets, blob_offset, value_ids_selvec, i,
	                                 VariantLogicalType::VARIANT_NULL);
}

template <bool WRITE_DATA, bool IGNORE_NULLS = false>
bool ConvertToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                      optional_ptr<SelectionVector> selvec, SelectionVector &keys_selvec,
                      OrderedOwningStringMap<uint32_t> &dictionary, optional_ptr<SelectionVector> value_ids_selvec,
                      const bool is_root);

} // namespace variant
} // namespace duckdb
