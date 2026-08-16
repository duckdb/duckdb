#include "duckdb/common/vector/array_vector.hpp"
#include "core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/storage/statistics/array_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

void ArrayValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	const auto &head = args.data[0];
	const auto &tail = ArgumentPack::GetInput(args.data[1]);

	const auto num_rows = args.size();
	const auto num_cols = 1 + tail.size();

	auto &child = ArrayVector::GetChildMutable(result);

	FlatVector::ValidityMutable(child).Resize(num_rows * num_cols);

	for (idx_t i = 0; i < num_rows; i++) {
		const auto &head_val = head.GetValue(i);
		child.SetValue(i * num_cols, head_val);

		for (idx_t j = 0; j < tail.size(); j++) {
			const auto &tail_val = tail[j].GetValue(i);
			child.SetValue((i * num_cols) + 1 + j, tail_val);
		}
	}

	result.Verify();
}

unique_ptr<FunctionData> ArrayValueBind(BindScalarFunctionInput &input) {
	auto &func = input.GetBoundFunction();

	const auto &args = input.GetArguments();
	const auto &head = args[0]->GetReturnType();
	const auto &tail = ArgumentPack::GetTypes(args[1]->GetReturnType());

	if (tail.size() > ArrayType::MAX_ARRAY_SIZE) {
		throw OutOfRangeException("Array size exceeds maximum allowed size");
	}

	func.SetReturnType(LogicalType::ARRAY(head, 1 + tail.size()));
	return make_uniq<VariableReturnBindData>(func.GetReturnType());
}

unique_ptr<BaseStatistics> ArrayValueStats(ClientContext &, FunctionStatisticsInput &input) {

	const auto &head_stats = input.child_stats[0];
	const auto &tail_stats = input.child_stats[1];
	const auto tail_length = ArgumentPack::GetSize(tail_stats.GetType());

	auto array_stats = ArrayStats::CreateEmpty(input.expr.GetReturnType());
	auto &elem_stats = ArrayStats::GetChildStats(array_stats);

	elem_stats.Merge(head_stats);
	for (idx_t i = 0; i < tail_length; i++) {
		elem_stats.Merge(StructStats::GetChildStats(tail_stats, i));
	}

	array_stats.SetHasNoNullFast();
	return array_stats.ToUnique();
}

} // namespace

ScalarFunction ArrayValueFun::GetFunction() {
	const auto element_type = LogicalType::TEMPLATE("T");

	auto sig = FunctionSignature()
		.AddParameter("head", element_type)
		.AddVarPositionalParameter("tail", element_type)
		.SetReturnType(LogicalType::ARRAY(element_type, optional_idx()));

	auto fun = ScalarFunction("array_value", std::move(sig))
		.SetFunctionCallback(ArrayValueFunction)
		.SetBindCallback(ArrayValueBind)
		.SetStatisticsCallback(ArrayValueStats)
		.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return fun;
}

} // namespace duckdb
