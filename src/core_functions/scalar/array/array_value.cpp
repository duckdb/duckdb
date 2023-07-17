#include "duckdb/core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void ArrayValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::ARRAY);
	auto &child_type = ArrayType::GetChildType(result.GetType());

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	D_ASSERT(args.ColumnCount() == ArrayType::GetSize(result.GetType()));

	auto &child = ArrayVector::GetEntry(result);
	for (idx_t i = 0; i < args.size(); i++) {
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).DefaultCastAs(child_type);
			child.SetValue(col_idx, val);
		}
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> ArrayValueBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	LogicalType child_type = arguments.empty() ? LogicalType::SQLNULL : arguments[0]->return_type;
	for (idx_t i = 1; i < arguments.size(); i++) {
		child_type = LogicalType::MaxLogicalType(child_type, arguments[i]->return_type);
	}

	if(arguments.size() >= NumericLimits<uint32_t>::Maximum()) {
		throw Exception("Too many arguments for array_value");
	}

	// this is more for completeness reasons
	bound_function.varargs = child_type;
	bound_function.return_type = LogicalType::ARRAY(child_type, arguments.size());
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

/*
unique_ptr<BaseStatistics> ListValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ListStats::CreateEmpty(expr.return_type);
	auto &list_child_stats = ListStats::GetChildStats(list_stats);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		list_child_stats.Merge(child_stats[i]);
	}
	return list_stats.ToUnique();
}
*/

ScalarFunction ArrayValueFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("array_value", {}, LogicalTypeId::ARRAY, ArrayValueFunction, ArrayValueBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
