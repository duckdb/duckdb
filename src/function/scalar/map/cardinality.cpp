#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

static void CardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &map = args.data[0];
	UnifiedVectorFormat list_data;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<uint64_t>(result);

	auto &children = StructVector::GetEntries(map);
	children[0]->ToUnifiedFormat(args.size(), list_data);
	for (idx_t row = 0; row < args.size(); row++) {
		auto list_entry = ((list_entry_t *)list_data.data)[list_data.sel->get_index(row)];
		result_data[row] = list_entry.length;
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
	ScalarFunction fun("cardinality", {LogicalType::ANY}, LogicalType::UBIGINT, CardinalityFunction, CardinalityBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(fun);
}

} // namespace duckdb
