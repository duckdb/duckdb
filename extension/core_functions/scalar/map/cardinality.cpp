#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void CardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &map = args.data[0];
	UnifiedVectorFormat map_data;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<uint64_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	map.ToUnifiedFormat(args.size(), map_data);
	for (idx_t row = 0; row < args.size(); row++) {
		auto list_entry = UnifiedVectorFormat::GetData<list_entry_t>(map_data)[map_data.sel->get_index(row)];
		result_data[row] = list_entry.length;
		result_validity.Set(row, map_data.validity.RowIsValid(map_data.sel->get_index(row)));
	}

	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> CardinalityBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw BinderException("Cardinality must have exactly one arguments");
	}

	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("Cardinality can only operate on MAPs");
	}

	bound_function.return_type = LogicalType::UBIGINT;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction CardinalityFun::GetFunction() {
	ScalarFunction fun({LogicalType::ANY}, LogicalType::UBIGINT, CardinalityFunction, CardinalityBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	return fun;
}

} // namespace duckdb
