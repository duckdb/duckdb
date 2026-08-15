#include "duckdb/common/vector/array_vector.hpp"
#include "core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/storage/statistics/array_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

void ArrayValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto array_type = result.GetType();

	auto &values = StructVector::GetEntries(args.data[0]);

	D_ASSERT(array_type.id() == LogicalTypeId::ARRAY);
	D_ASSERT(values.size() == ArrayType::GetSize(array_type));

	auto &child_type = ArrayType::GetChildType(array_type);

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (auto &value : values) {
		if (value.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto num_rows = args.size();
	auto num_columns = values.size();

	auto &child = ArrayVector::GetChildMutable(result);

	if (num_columns > 1) {
		// Ensure that the child has a validity mask of the correct size
		// The SetValue call below expects the validity mask to be initialized
		auto &child_validity = FlatVector::ValidityMutable(child);
		child_validity.Resize(num_rows * num_columns);
	}

	for (idx_t i = 0; i < num_rows; i++) {
		for (idx_t j = 0; j < num_columns; j++) {
			auto val = values[j].GetValue(i).DefaultCastAs(child_type);
			child.SetValue((i * num_columns) + j, val);
		}
	}

	result.Verify();
}

unique_ptr<FunctionData> ArrayValueBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	D_ASSERT(arguments.size() == 1);
	auto &values = StructType::GetChildTypes(arguments[0]->GetReturnType());
	if (values.empty()) {
		throw InvalidInputException("array_value requires at least one argument");
	}

	// construct return type
	LogicalType child_type = values[0].second;
	for (idx_t i = 1; i < values.size(); i++) {
		child_type = LogicalType::MaxLogicalType(context, child_type, values[i].second);
	}

	if (values.size() > ArrayType::MAX_ARRAY_SIZE) {
		throw OutOfRangeException("Array size exceeds maximum allowed size");
	}

	// Cast all arguments to the common child type so that execution and statistics see matching types.
	const auto value_count = values.size();
	bound_function.GetArguments()[0] = ArgumentPack::PositionalType(vector<LogicalType>(value_count, child_type));

	bound_function.SetReturnType(LogicalType::ARRAY(child_type, value_count));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

unique_ptr<BaseStatistics> ArrayValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ArrayStats::CreateEmpty(expr.GetReturnType());
	auto &list_child_stats = ArrayStats::GetChildStats(list_stats);
	// the values are collected by a "*values" parameter, so their statistics are the pack's member statistics
	D_ASSERT(child_stats.size() == 1);
	const auto value_count = StructType::GetChildCount(expr.GetChildren()[0]->GetReturnType());
	for (idx_t i = 0; i < value_count; i++) {
		list_child_stats.Merge(StructStats::GetChildStats(child_stats[0], i));
	}
	list_stats.SetHasNoNullFast();
	return list_stats.ToUnique();
}

} // namespace

ScalarFunction ArrayValueFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	FunctionSignature signature;
	signature.AddVarPositionalParameter("values", LogicalType::ANY);
	signature.SetReturnType(LogicalTypeId::ARRAY);
	ScalarFunction fun("array_value", std::move(signature), ArrayValueFunction);
	fun.SetBindCallback(ArrayValueBind);
	fun.SetStatisticsCallback(ArrayValueStats);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
