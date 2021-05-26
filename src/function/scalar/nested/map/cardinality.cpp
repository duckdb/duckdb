#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

static void CardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &map = args.data[0];
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<uint64_t>(result);
	if (!StructVector::HasEntries(map)) {
		result_data[0] = 0;
		return;
	}
	auto &children = StructVector::GetEntries(map);
	for (idx_t row = 0; row < args.size(); row++) {
		auto list_data = FlatVector::GetData<list_entry_t>(*children[0].second);
		result_data[row] = list_data[row].length;
	}
	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> CardinalityBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {

	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("Cardinality can only operate on MAPs");
	}

	bound_function.return_type = LogicalType::UBIGINT;
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void CardinalityFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction fun("cardinality", {LogicalType::ANY}, LogicalType::UBIGINT, CardinalityFunction, false,
	                   CardinalityBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
