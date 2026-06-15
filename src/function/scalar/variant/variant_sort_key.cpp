#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/common/enums/order_type.hpp"

namespace duckdb {

// variant_sort_key produces a binary, byte-comparable sort key for a VARIANT value.
// It is used as a collation for VARIANT so that comparison / ordering / grouping all operate on the
// logical ordering of variant values (see EncodeVariantValue in create_sort_key.cpp).
// Unlike the create_sort_key scalar (which uses SPECIAL_HANDLING and encodes NULLs into the key), this
// function propagates NULL - this is required so that NULL = NULL stays NULL and ORDER BY ... NULLS
// FIRST/LAST is handled by the surrounding operator instead of being baked into the key.
static void VariantSortKeyFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);
	OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
	// CreateVariantSortKey encodes the logical value of the variant and propagates NULL into the
	// result validity (NULL = NULL stays NULL, and ORDER BY ... NULLS FIRST/LAST is handled by the operator)
	CreateSortKeyHelpers::CreateVariantSortKey(input.data[0], input.size(), modifiers, result);
}

ScalarFunction VariantSortKeyFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("variant_sort_key", {variant_type}, LogicalType::BLOB, VariantSortKeyFunction);
}

} // namespace duckdb
