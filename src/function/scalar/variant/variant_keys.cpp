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

	vector<string_t> keys;
	for (const auto &entry : node->GetObjectChildren(VariantIterationOrder::LEXICOGRAPHIC)) {
		keys.push_back(entry.key);
	}

	auto keys_writer = list_writer.WriteList(keys.size());
	idx_t key_idx = 0;
	for (auto &key_writer : keys_writer) {
		key_writer.WriteValue(keys[key_idx++]);
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
