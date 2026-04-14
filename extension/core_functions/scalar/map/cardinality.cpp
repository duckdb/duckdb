#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void CardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &map = args.data[0];
	auto entries = map.Values<list_entry_t>(args.size());

	auto result_data = FlatVector::Writer<uint64_t>(result, args.size());
	for (idx_t row = 0; row < args.size(); row++) {
		auto entry = entries[row];
		if (!entry.IsValid()) {
			result_data.SetInvalid(row);
			continue;
		}
		result_data[row] = entries.GetValueUnsafe(row).length;
	}
}

static unique_ptr<FunctionData> CardinalityBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments.size() != 1) {
		throw BinderException("Cardinality must have exactly one arguments");
	}

	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("Cardinality can only operate on MAPs");
	}

	bound_function.SetReturnType(LogicalType::UBIGINT);
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

ScalarFunction CardinalityFun::GetFunction() {
	ScalarFunction fun({LogicalType::ANY}, LogicalType::UBIGINT, CardinalityFunction, CardinalityBind);
	fun.varargs = LogicalType::ANY;
	fun.SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING);
	return fun;
}

} // namespace duckdb
