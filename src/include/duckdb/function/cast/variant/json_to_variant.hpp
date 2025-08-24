#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertJSONToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                          optional_ptr<SelectionVector> selvec, SelectionVector &keys_selvec,
                          OrderedOwningStringMap<uint32_t> &dictionary, optional_ptr<SelectionVector> value_ids_selvec,
                          const bool is_root) {
	throw NotImplementedException("JSON to VARIANT not implemented");
}

} // namespace variant
} // namespace duckdb
