#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

static void WriteKeysResult(const optional<VariantNode> &node, VectorWriter<VectorListType<string_t>> &list_writer) {
	if (!node) {
		list_writer.WriteNull();
		return;
	}
	if (node->GetTypeId() != VariantLogicalType::OBJECT) {
		list_writer.WriteList(0);
		return;
	}

	auto keys_writer = list_writer.WriteDynamicList();
	for (const auto &entry : node->GetObjectChildren(VariantIterationOrder::LEXICOGRAPHIC)) {
		keys_writer.WriteElement().WriteValue(entry.key);
	}
}

static void VariantKeysFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<VectorListType<string_t>>(input, state, result, WriteKeysResult);
}

ScalarFunctionSet VariantKeysFun::GetFunctions() {
	return VariantPathFunction::CreateFunctionSet("variant_keys", VariantKeysFunction,
	                                              LogicalType::LIST(LogicalType::VARCHAR));
}

} // namespace duckdb
