#include "duckdb/core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

// TODO: Handle nulls
static void ArrayCosineSimilarityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &lhs = args.data[0];
	auto &rhs = args.data[1];

	auto array_size = ArrayType::GetSize(lhs.GetType());

	auto &lhs_child = ArrayVector::GetEntry(lhs);
	auto &rhs_child = ArrayVector::GetEntry(rhs);

	auto lhs_data = FlatVector::GetData<double>(lhs_child);
	auto rhs_data = FlatVector::GetData<double>(rhs_child);
	auto res_data = FlatVector::GetData<double>(result);

	for (idx_t i = 0; i < count; i++) {
		double dot = 0;
		double denom_l = 0;
		double denom_r = 0;
		for (idx_t j = i * array_size; j < (i + 1) * array_size; j++) {
			auto x = lhs_data[j];
			auto y = rhs_data[j];
			denom_l += x * x;
			denom_r += y * y;
			dot += x * y;
		}
		auto res = dot / (std::sqrt(denom_l) * std::sqrt(denom_r));
		res_data[i] = res;
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ArrayCosineSimilarityBind(ClientContext &context, ScalarFunction &bound_function,
                                                          vector<unique_ptr<Expression>> &arguments) {
	if (ArrayType::GetSize(arguments[0]->return_type) != ArrayType::GetSize(arguments[1]->return_type)) {
		throw Exception("Array size must be equal");
	}
	if (ArrayType::GetChildType(arguments[0]->return_type) != ArrayType::GetChildType(arguments[1]->return_type)) {
		throw Exception("Array type must be equal");
	}
	auto size = ArrayType::GetSize(arguments[0]->return_type);
	if (ArrayType::GetChildType(arguments[0]->return_type) != LogicalTypeId::DOUBLE) {
		arguments[0] = BoundCastExpression::AddCastToType(context, std::move(arguments[0]),
		                                                  LogicalType::ARRAY(LogicalType::DOUBLE, size));
	}
	if (ArrayType::GetChildType(arguments[1]->return_type) != LogicalTypeId::DOUBLE) {
		arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]),
		                                                  LogicalType::ARRAY(LogicalType::DOUBLE, size));
	}

	bound_function.arguments.push_back(arguments[0]->return_type);
	bound_function.arguments.push_back(arguments[1]->return_type);
	return nullptr;
}

ScalarFunctionSet ArrayCosineSimilarityFun::GetFunctions() {

	ScalarFunctionSet set;

	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("array_cosine_similarity", {}, LogicalType::DOUBLE, ArrayCosineSimilarityFunction,
	                   ArrayCosineSimilarityBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);

	return set;
}

} // namespace duckdb
