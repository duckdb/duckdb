#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/cast/variant/to_variant_fwd.hpp"
#include "duckdb/common/types/variant_value.hpp"

namespace duckdb {

static vector<VariantValue> Unshred(Vector &unshredded, Vector &shredded, idx_t count) {
	vector<VariantValue> res;

	D_ASSERT(shredded.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_entries = StructVector::GetEntries(shredded);
	D_ASSERT(child_entries.size() == 2);

	auto &untyped_value_index = *child_entries[0];
	auto &typed_value = *child_entries[1];

	//! Rows that are NULL in 'shredded' *only* have unshredded data
	//! Rows that are NULL in unshredded data *only* have fully shredded data
	//! Rows that are non-NULL in both, contain both unshredded and shredded data (partial shredding)
}

void VariantColumnData::UnshredVariantData(Vector &input, Vector &output, idx_t count) {
	D_ASSERT(input.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_vectors = StructVector::GetEntries(input);
	D_ASSERT(child_vectors.size() == 2);

	auto &unshredded = *child_vectors[0];
	auto &shredded = *child_vectors[1];

	auto values = Unshred(unshredded, shredded, count);
}

} // namespace duckdb
