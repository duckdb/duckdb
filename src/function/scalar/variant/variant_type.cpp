#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"

namespace duckdb {

static Vector CollectVariantTypes(const UnifiedVariantVectorData &variant,
                                  const vector<VariantPathComponent> &components, const idx_t count) {
	Vector types(LogicalType::VARCHAR, count);

	return types;
}

static void WriteTypeResult(const UnifiedVariantVectorData &, VectorWriter<string_t> &writer, const Vector &vector,
                            const idx_t row_idx) {
}

static void VariantTypeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<Vector, string_t>(input, state, result, CollectVariantTypes, WriteTypeResult);
}

ScalarFunctionSet VariantTypeFun::GetFunctions() {
	ScalarFunctionSet fun_set;

	ScalarFunction variant_type("variant_type", {}, LogicalType::VARCHAR,
	                            VariantTypeFunction, VariantBindUtils::VariantPathBind, nullptr);

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
