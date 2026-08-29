#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

static void WriteArrayLengthsResult(const optional<VariantNode> &node, VectorWriter<uint64_t> &length_writer) {
	if (!node) {
		length_writer.WriteNull();
		return;
	}
	if (node->GetTypeId() != VariantLogicalType::ARRAY) {
		length_writer.WriteValue(0);
		return;
	}

	length_writer.WriteValue(node->GetArrayChildren().size());
}

static void VariantArrayLengthFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<uint64_t>(input, state, result, WriteArrayLengthsResult);
}

ScalarFunctionSet VariantArrayLengthFun::GetFunctions() {
	return VariantPathFunction::CreateFunctionSet("variant_array_length", VariantArrayLengthFunction,
	                                              LogicalType::UBIGINT);
}

} // namespace duckdb
