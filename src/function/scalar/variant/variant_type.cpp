#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"

namespace duckdb {

static void WriteTypeResult(const optional<VariantNode> &node, VectorWriter<string_t> &types_writer) {
	if (!node) {
		types_writer.WriteNull();
		return;
	}

	const auto type = node->GetTypeId();
	types_writer.WriteValue(string_t(EnumUtil::ToString(type)));
}

static void VariantTypeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<string_t>(input, state, result, WriteTypeResult);
}

ScalarFunctionSet VariantTypeFun::GetFunctions() {
	return VariantPathFunction::CreateFunctionSet("variant_type", VariantTypeFunction, LogicalType::VARCHAR);
}

} // namespace duckdb
