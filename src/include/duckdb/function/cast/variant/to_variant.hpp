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
//! * @param count The amount of values we're converting
//! * @param source_size The size of the source vector
//! * @param selvec The selection vector from i (< count) to the index in the result Vector
//! * @param source_sel The selection vector from i (< count) to the index in the source Vector, if not null
//! * @param keys_selvec The selection vector to populate with mapping from keys index -> dictionary index
//! * @param dictionary The dictionary to populate with the (unique and sorted) keys
//! * @param values_index_selvec The selection vector from i (< count) to the index in the children.values_index selvec,
//! to populate the parent's children
//! * @param is_root Whether we are writing to the root of the Variant, or a child value (in an OBJECT/ARRAY)
template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count, idx_t source_size,
                      optional_ptr<const SelectionVector> selvec, optional_ptr<const SelectionVector> source_sel,
                      SelectionVector &keys_selvec, OrderedOwningStringMap<uint32_t> &dictionary,
                      optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {
	auto &type = source.GetType();

	auto logical_type = type.id();
	if (type.IsNested()) {
		switch (logical_type) {
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST:
			return ConvertListToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, source_size, selvec,
			                                                      source_sel, keys_selvec, dictionary,
			                                                      values_index_selvec, is_root);
		case LogicalTypeId::ARRAY:
			return ConvertArrayToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, source_size, selvec,
			                                                       source_sel, keys_selvec, dictionary,
			                                                       values_index_selvec, is_root);
		case LogicalTypeId::STRUCT:
			return ConvertStructToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, source_size, selvec,
			                                                        source_sel, keys_selvec, dictionary,
			                                                        values_index_selvec, is_root);
		case LogicalTypeId::UNION:
			return ConvertUnionToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, source_size, selvec,
			                                                       source_sel, keys_selvec, dictionary,
			                                                       values_index_selvec, is_root);
		case LogicalTypeId::VARIANT:
			return ConvertVariantToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, source_size,
			                                                         selvec, source_sel, keys_selvec, dictionary,
			                                                         values_index_selvec, is_root);
		default:
			throw NotImplementedException("Can't convert nested type '%s'", EnumUtil::ToString(logical_type));
		};
	} else {
		if (type.IsJSONType()) {
			return ConvertJSONToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, source_size, selvec,
			                                                      source_sel, keys_selvec, dictionary,
			                                                      values_index_selvec, is_root);
		} else {
			return ConvertPrimitiveToVariant<WRITE_DATA, IGNORE_NULLS>(source, result, offsets, count, source_size,
			                                                           selvec, source_sel, keys_selvec, dictionary,
			                                                           values_index_selvec, is_root);
		}
	}
	return true;
}

} // namespace variant
} // namespace duckdb
