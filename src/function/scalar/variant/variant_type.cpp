#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"

namespace duckdb {

static string FormatType(const VariantLogicalType &type) {
	switch (type) {
	case VariantLogicalType::OBJECT:
		return "OBJECT";
	case VariantLogicalType::ARRAY:
		return "ARRAY";
	default:
		return EnumUtil::ToString(type);
	}
}

static void WriteTypes(const UnifiedVariantVectorData &variant, Vector &types, SelectionVector value_index_sel,
                       const idx_t count) {
	auto writer = FlatVector::Writer<string_t>(types, count);
	const auto &types_validity = FlatVector::Validity(types);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!types_validity.RowIsValid(row_idx)) {
			writer.WriteNull();
			continue;
		}

		auto type = variant.GetTypeId(row_idx, value_index_sel[row_idx]);
		writer.WriteValue(string_t(FormatType(type)));
	}
}

static Vector CollectVariantTypes(const UnifiedVariantVectorData &variant,
                                  const vector<VariantPathComponent> &components, const idx_t count) {
	Vector types(LogicalType::VARCHAR, count);
	VariantPathSelection path_selection(count);

	const auto owned_nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);
	const array_ptr nested_data(owned_nested_data.get(), count);

	auto &type_validity = FlatVector::ValidityMutable(types);
	VariantUtils::TraversePath(variant, components, count, nested_data, type_validity, path_selection);

	const auto value_index_sel = path_selection.Input(components.size());
	WriteTypes(variant, types, value_index_sel, count);

	return types;
}

static void WriteTypeResult(const UnifiedVariantVectorData &, VectorWriter<string_t> &types_writer, const Vector &types,
                            const idx_t row_idx) {
	const auto &types_validity = FlatVector::Validity(types);
	const auto types_data = FlatVector::GetData<string_t>(types);

	if (!types_validity.RowIsValid(row_idx)) {
		types_writer.WriteNull();
		return;
	}

	const auto type = types_data[row_idx];
	types_writer.WriteValue(type);
}

static void VariantTypeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<Vector, string_t>(input, state, result, CollectVariantTypes, WriteTypeResult);
}

ScalarFunctionSet VariantTypeFun::GetFunctions() {
	ScalarFunctionSet fun_set;

	ScalarFunction variant_type("variant_type", {}, LogicalType::VARCHAR, VariantTypeFunction,
	                            VariantBindUtils::VariantPathBind, nullptr);

	variant_type.GetSignature().AddParameter(LogicalType::VARIANT());
	fun_set.AddFunction(variant_type);

	variant_type.GetSignature().AddParameter(LogicalType::VARCHAR);
	fun_set.AddFunction(variant_type);

	variant_type.GetSignature().GetParameter(1).SetType(LogicalType::LIST(LogicalType::VARCHAR));
	variant_type.SetReturnType(LogicalType::LIST(LogicalType::BOOLEAN));
	fun_set.AddFunction(variant_type);

	return fun_set;
}

} // namespace duckdb
