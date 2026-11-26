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
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
		return false;
	default:
		return true;
	}
}

void VariantValueFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::ExecuteVariant(input, state, result, IsScalarValue);
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
