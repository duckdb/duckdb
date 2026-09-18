#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

static void WriteExistsResult(const optional<VariantNode> &node, VectorWriter<bool> &existence_writer) {
	if (!node) {
		existence_writer.WriteValue(false);
		return;
	}
	existence_writer.WriteValue(true);
}

static void VariantExistsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<bool>(input, state, result, WriteExistsResult);
}

ScalarFunctionSet VariantExistsFun::GetFunctions() {
	return VariantPathFunction::CreateFunctionSet("variant_exists", VariantExistsFunction, LogicalType::BOOLEAN, false);
}

} // namespace duckdb
