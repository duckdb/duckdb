#pragma once

#include "duckdb/function/cast/variant/list_to_variant.hpp"
#include "duckdb/function/cast/variant/array_to_variant.hpp"
#include "duckdb/function/cast/variant/json_to_variant.hpp"
#include "duckdb/function/cast/variant/struct_to_variant.hpp"
#include "duckdb/function/cast/variant/union_to_variant.hpp"
#include "duckdb/function/cast/variant/variant_to_variant.hpp"
#include "duckdb/function/cast/variant/primitive_to_variant.hpp"

namespace duckdb {
namespace variant {

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
bool ConvertToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                      optional_ptr<const SelectionVector> selvec, SelectionVector &keys_selvec,
                      OrderedOwningStringMap<uint32_t> &dictionary,
                      optional_ptr<const SelectionVector> value_ids_selvec, const bool is_root) {
	auto &type = source.GetType();

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
		if (type.IsJSONType()) {
			return ConvertJSONToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, selvec, keys_selvec,
			                                                      dictionary, value_ids_selvec, is_root);
		} else {
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS>(
			    source, result, offsets, count, selvec, keys_selvec, dictionary, value_ids_selvec, is_root);
		}
	}
	return true;
}

} // namespace variant
} // namespace duckdb
