#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

namespace {

//! Whether the selected value is returned, OBJECT, ARRAY and VARIANT_NULL values are not
bool IsScalarValue(const VariantNode &node) {
	if (node.IsNull() || node.IsMissing()) {
		return false;
	}
	switch (node.GetTypeId()) {
	case VariantLogicalType::VARIANT_NULL:
	case VariantLogicalType::OBJECT:
	case VariantLogicalType::ARRAY:
		return false;
	default:
		return true;
	}
}

//! Writes the selected value, or a NULL if it is missing or not a scalar
void WriteValueResult(const optional<VariantNode> &node, VectorWriter<VectorVariantType> &value_writer) {
	if (!node || !IsScalarValue(*node)) {
		value_writer.WriteNull();
		return;
	}
	value_writer.WriteValue(*node);
}

void VariantValueFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<VectorVariantType>(input, state, result, WriteValueResult);
}

} // namespace

ScalarFunctionSet VariantValueFun::GetFunctions() {
	auto set =
	    VariantPathFunction::CreateFunctionSet("variant_value", VariantValueFunction, LogicalType::VARIANT(), false);

	ScalarFunction index_fun({}, LogicalType::VARIANT(), VariantValueFunction, VariantBindUtils::VariantPathBind);
	index_fun.GetSignature()
	    .AddParameter("input_variant", LogicalType::VARIANT())
	    .AddParameter("index", LogicalType::BIGINT);
	set.AddFunction(std::move(index_fun));
	return set;
}

} // namespace duckdb
