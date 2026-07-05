#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

// TODO: Currently collection will always happen on the unshredded variant, introduce a fast path for shredded variants.
static ValidityMask CollectVariantExistence(const UnifiedVariantVectorData &variant,
                                            const vector<VariantPathComponent> &components, const idx_t count) {
	ValidityMask path_validity(count);
	VariantPathSelection path_selection(count);

	const auto owned_nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);
	const array_ptr nested_data(owned_nested_data.get(), count);

	VariantUtils::TraversePath(variant, components, count, nested_data, path_validity, path_selection);

	return path_validity;
}

static void WriteExistsResult(const UnifiedVariantVectorData &, VectorWriter<bool> &existence_writer,
                              const ValidityMask &path_validity, const idx_t row_idx) {
	if (path_validity.RowIsValid(row_idx)) {
		existence_writer.WriteValue(true);
	} else {
		existence_writer.WriteValue(false);
	}
}

static void VariantExistsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<ValidityMask, bool>(input, state, result, CollectVariantExistence, WriteExistsResult);
}

ScalarFunctionSet VariantExistsFun::GetFunctions() {
	ScalarFunctionSet fun_set;

	ScalarFunction variant_exists("variant_exists", {LogicalType::VARIANT(), LogicalType::VARCHAR},
	                              LogicalType::BOOLEAN, VariantExistsFunction, VariantBindUtils::VariantPathBind,
	                              nullptr);
	fun_set.AddFunction(variant_exists);

	variant_exists.GetSignature().GetParameter(1).SetType(LogicalType::LIST(LogicalType::VARCHAR));
	variant_exists.SetReturnType(LogicalType::LIST(LogicalType::BOOLEAN));
	fun_set.AddFunction(variant_exists);

	return fun_set;
}

} // namespace duckdb
