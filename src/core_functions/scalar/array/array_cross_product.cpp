#include "duckdb/core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include <cmath>

namespace duckdb {

template <class FLOAT_TYPE>
static void ArrayCrossProductFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &lhs = args.data[0];
	auto &rhs = args.data[1];

	lhs.Flatten(count);
	rhs.Flatten(count);

	auto &lhs_child = ArrayVector::GetEntry(lhs);
	auto &rhs_child = ArrayVector::GetEntry(rhs);
	auto &res_child = ArrayVector::GetEntry(result);

	auto left_valid =
	    FlatVector::Validity(lhs_child).AllValid() || FlatVector::Validity(lhs_child).CheckAllValid(count * 3);
	auto right_valid =
	    FlatVector::Validity(rhs_child).AllValid() || FlatVector::Validity(rhs_child).CheckAllValid(count * 3);
	if (!left_valid || !right_valid) {
		throw InvalidInputException("Array cross product is not defined for null values");
	}

	auto res_data = FlatVector::GetData<FLOAT_TYPE>(res_child);
	auto lhs_data = FlatVector::GetData<FLOAT_TYPE>(lhs_child);
	auto rhs_data = FlatVector::GetData<FLOAT_TYPE>(rhs_child);

	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(lhs, i) || FlatVector::IsNull(rhs, i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto lhs_idx = i * 3;
		auto rhs_idx = i * 3;
		auto res_idx = i * 3;

		auto lx = lhs_data[lhs_idx + 0];
		auto ly = lhs_data[lhs_idx + 1];
		auto lz = lhs_data[lhs_idx + 2];

		auto rx = rhs_data[rhs_idx + 0];
		auto ry = rhs_data[rhs_idx + 1];
		auto rz = rhs_data[rhs_idx + 2];

		res_data[res_idx + 0] = ly * rz - lz * ry;
		res_data[res_idx + 1] = lz * rx - lx * rz;
		res_data[res_idx + 2] = lx * ry - ly * rx;
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		if (FlatVector::IsNull(lhs, 0) || FlatVector::IsNull(rhs, 0)) {
			ConstantVector::SetNull(result, true);
		}
	}
}

ScalarFunctionSet ArrayCrossProductFun::GetFunctions() {

	ScalarFunctionSet set;

	// Cross product is only defined for 3d vectors
	auto double_vec = LogicalType::ARRAY(LogicalType::DOUBLE, 3);
	ScalarFunction double_fun("array_cross_product", {double_vec, double_vec}, double_vec,
	                          ArrayCrossProductFunction<double>);
	set.AddFunction(double_fun);

	auto float_vec = LogicalType::ARRAY(LogicalType::FLOAT, 3);
	ScalarFunction float_fun("array_cross_product", {float_vec, float_vec}, float_vec,
	                         ArrayCrossProductFunction<float>);
	set.AddFunction(float_fun);

	return set;
}

} // namespace duckdb
