#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

void VariantUtils::UnshredVariantData(Vector &input, Vector &output, idx_t count) {
	D_ASSERT(input.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_vectors = StructVector::GetEntries(input);
	D_ASSERT(child_vectors.size() == 2);

	auto &unshredded = child_vectors[0];
	auto &shredded = child_vectors[1];

	//! Traverse the (shredded) variant directly through the iterator and encode it into the canonical
	//! unshredded layout - no intermediate vector<VariantValue> materialization is required.
	VariantIterator state(unshredded, shredded);
	ToVariant(state, count, output);
}

} // namespace duckdb
