#include "core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/storage/statistics/array_stats.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

void ArrayValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto array_type = result.GetType();

	D_ASSERT(array_type.id() == LogicalTypeId::ARRAY);
	D_ASSERT(args.ColumnCount() == ArrayType::GetSize(array_type));

	auto &child_type = ArrayType::GetChildType(array_type);

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto num_rows = args.size();
	auto num_columns = args.ColumnCount();

	auto &child = ArrayVector::GetEntry(result);

	if (num_columns > 1) {
		// Ensure that the child has a validity mask of the correct size
		// The SetValue call below expects the validity mask to be initialized
		auto &child_validity = FlatVector::Validity(child);
		child_validity.Resize(num_rows * num_columns);
	}

	for (idx_t i = 0; i < num_rows; i++) {
		for (idx_t j = 0; j < num_columns; j++) {
			auto val = args.GetValue(j, i).DefaultCastAs(child_type);
			child.SetValue((i * num_columns) + j, val);
		}
	}

	result.Verify(args.size());
}

unique_ptr<FunctionData> ArrayValueBind(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw InvalidInputException("array_value requires at least one argument");
	}

	// construct return type
	LogicalType child_type = arguments[0]->return_type;
	for (idx_t i = 1; i < arguments.size(); i++) {
		child_type = LogicalType::MaxLogicalType(context, child_type, arguments[i]->return_type);
	}

	if (arguments.size() > ArrayType::MAX_ARRAY_SIZE) {
		throw OutOfRangeException("Array size exceeds maximum allowed size");
	}

	// this is more for completeness reasons
	bound_function.varargs = child_type;
	bound_function.SetReturnType(LogicalType::ARRAY(child_type, arguments.size()));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

unique_ptr<BaseStatistics> ArrayValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ArrayStats::CreateEmpty(expr.return_type);
	auto &list_child_stats = ArrayStats::GetChildStats(list_stats);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		list_child_stats.Merge(child_stats[i]);
	}
	return list_stats.ToUnique();
}

} // namespace

ScalarFunction ArrayValueFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("array_value", {}, LogicalTypeId::ARRAY, ArrayValueFunction, ArrayValueBind, nullptr,
	                   ArrayValueStats);
	fun.varargs = LogicalType::ANY;
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
