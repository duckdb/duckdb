#include "duckdb/common/vector/array_vector.hpp"
#include "core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/storage/statistics/array_stats.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
class ClientContext;
struct ExpressionState;

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

	auto &child = ArrayVector::GetChildMutable(result);

	if (num_columns > 1) {
		// Ensure that the child has a validity mask of the correct size
		// The SetValue call below expects the validity mask to be initialized
		auto &child_validity = FlatVector::ValidityMutable(child);
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

unique_ptr<FunctionData> ArrayValueBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments.empty()) {
		throw InvalidInputException("array_value requires at least one argument");
	}

	// construct return type
	LogicalType child_type = arguments[0]->GetReturnType();
	for (idx_t i = 1; i < arguments.size(); i++) {
		child_type = LogicalType::MaxLogicalType(context, child_type, arguments[i]->GetReturnType());
	}

	if (arguments.size() > ArrayType::MAX_ARRAY_SIZE) {
		throw OutOfRangeException("Array size exceeds maximum allowed size");
	}

	// this is more for completeness reasons
	bound_function.SetVarArgs(child_type);
	bound_function.SetReturnType(LogicalType::ARRAY(child_type, arguments.size()));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

unique_ptr<BaseStatistics> ArrayValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ArrayStats::CreateEmpty(expr.GetReturnType());
	auto &list_child_stats = ArrayStats::GetChildStats(list_stats);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		list_child_stats.Merge(child_stats[i]);
	}
	list_stats.SetHasNoNullFast();
	return list_stats.ToUnique();
}

} // namespace

ScalarFunction ArrayValueFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("array_value", {}, LogicalTypeId::ARRAY, ArrayValueFunction, ArrayValueBind, ArrayValueStats);
	fun.SetVarArgs(LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
