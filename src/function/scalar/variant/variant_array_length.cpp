#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

static void WriteArrayLengths(Vector &array_lengths, const array_ptr<VariantNestedData> &nested_data,
                              const ValidityMask &array_validity, const idx_t count) {
	auto writer = FlatVector::Writer<uint64_t>(array_lengths, count);
	const auto &array_lengths_validity = FlatVector::Validity(array_lengths);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!array_lengths_validity.RowIsValid(row_idx)) {
			writer.WriteNull();
			continue;
		}
		if (!array_validity.RowIsValid(row_idx)) {
			writer.WriteValue(0);
			continue;
		}

		const auto &[child_count, children_idx] = nested_data[row_idx];
		writer.WriteValue(child_count);
	}
}

static Vector CollectVariantArrayLengths(const UnifiedVariantVectorData &variant,
                                         const vector<VariantPathComponent> &components, const idx_t count) {
	Vector array_lengths(LogicalType::UBIGINT, count);
	VariantPathSelection path_selection(count);

	const auto owned_nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);
	const array_ptr nested_data(owned_nested_data.get(), count);

	auto &path_validity = FlatVector::ValidityMutable(array_lengths);
	VariantUtils::TraversePath(variant, components, count, nested_data, path_validity, path_selection);

	// For the final collection of nested_data we use an auxiliary validity vector so we can distinguish "path missing"
	// from "path exists but not an array" (producing NULL and 0 as output respectively).
	ValidityMask array_validity(count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!path_validity.RowIsValid(row_idx)) {
			array_validity.SetInvalid(row_idx);
		}
	}

	const auto &final_indices = path_selection.Input(components.size());
	(void)VariantUtils::CollectNestedData(variant, VariantLogicalType::ARRAY, final_indices, count, optional_idx(), 0,
	                                      nested_data, array_validity);

	WriteArrayLengths(array_lengths, nested_data, array_validity, count);

	return array_lengths;
}

static void WriteArrayLengthsResult(const UnifiedVariantVectorData &, VectorWriter<uint64_t> &lengths_writer,
                                    const Vector &array_lengths, const idx_t row_idx) {
	const auto &array_lengths_validity = FlatVector::Validity(array_lengths);
	const auto array_lengths_data = FlatVector::GetData<const uint64_t>(array_lengths);

	if (!array_lengths_validity.RowIsValid(row_idx)) {
		lengths_writer.WriteNull();
		return;
	}

	const auto array_length = array_lengths_data[row_idx];
	lengths_writer.WriteValue(array_length);
}

static void VariantArrayLengthFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<Vector, uint64_t>(input, state, result, CollectVariantArrayLengths,
	                                               WriteArrayLengthsResult);
}

ScalarFunctionSet VariantArrayLengthFun::GetFunctions() {
	ScalarFunctionSet fun_set;

	ScalarFunction variant_exists("variant_array_length", {LogicalType::VARIANT(), LogicalType::VARCHAR},
	                              LogicalType::UBIGINT, VariantArrayLengthFunction, VariantBindUtils::VariantPathBind,
	                              nullptr);
	fun_set.AddFunction(variant_exists);

	variant_exists.GetSignature().GetParameter(1).SetType(LogicalType::LIST(LogicalType::VARCHAR));
	variant_exists.SetReturnType(LogicalType::LIST(LogicalType::UBIGINT));
	fun_set.AddFunction(variant_exists);

	return fun_set;
}

} // namespace duckdb
