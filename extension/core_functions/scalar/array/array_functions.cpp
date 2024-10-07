#include "core_functions/scalar/array_functions.hpp"
#include "core_functions/array_kernels.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ArrayGenericBinaryBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {

	const auto lhs_is_param = arguments[0]->HasParameter();
	const auto rhs_is_param = arguments[1]->HasParameter();

	if (lhs_is_param && rhs_is_param) {
		throw ParameterNotResolvedException();
	}

	const auto &lhs_type = arguments[0]->return_type;
	const auto &rhs_type = arguments[1]->return_type;

	bound_function.arguments[0] = lhs_is_param ? rhs_type : lhs_type;
	bound_function.arguments[1] = rhs_is_param ? lhs_type : rhs_type;

	if (bound_function.arguments[0].id() != LogicalTypeId::ARRAY ||
	    bound_function.arguments[1].id() != LogicalTypeId::ARRAY) {
		throw InvalidInputException(
		    StringUtil::Format("%s: Arguments must be arrays of FLOAT or DOUBLE", bound_function.name));
	}

	const auto lhs_size = ArrayType::GetSize(bound_function.arguments[0]);
	const auto rhs_size = ArrayType::GetSize(bound_function.arguments[1]);

	if (lhs_size != rhs_size) {
		throw BinderException("%s: Array arguments must be of the same size", bound_function.name);
	}

	const auto &lhs_element_type = ArrayType::GetChildType(bound_function.arguments[0]);
	const auto &rhs_element_type = ArrayType::GetChildType(bound_function.arguments[1]);

	// Resolve common type
	LogicalType common_type;
	if (!LogicalType::TryGetMaxLogicalType(context, lhs_element_type, rhs_element_type, common_type)) {
		throw BinderException("%s: Cannot infer common element type (left = '%s', right = '%s')", bound_function.name,
		                      lhs_element_type.ToString(), rhs_element_type.ToString());
	}

	// Ensure it is float or double
	if (common_type.id() != LogicalTypeId::FLOAT && common_type.id() != LogicalTypeId::DOUBLE) {
		throw BinderException("%s: Arguments must be arrays of FLOAT or DOUBLE", bound_function.name);
	}

	// The important part is just that we resolve the size of the input arrays
	bound_function.arguments[0] = LogicalType::ARRAY(common_type, lhs_size);
	bound_function.arguments[1] = LogicalType::ARRAY(common_type, rhs_size);

	return nullptr;
}

//------------------------------------------------------------------------------
// Element-wise combine functions
//------------------------------------------------------------------------------
// Given two arrays of the same size, combine their elements into a single array
// of the same size as the input arrays.

struct CrossProductOp {
	template <class TYPE>
	static void Operation(const TYPE *lhs_data, const TYPE *rhs_data, TYPE *res_data, idx_t size) {
		D_ASSERT(size == 3);

		auto lx = lhs_data[0];
		auto ly = lhs_data[1];
		auto lz = lhs_data[2];

		auto rx = rhs_data[0];
		auto ry = rhs_data[1];
		auto rz = rhs_data[2];

		res_data[0] = ly * rz - lz * ry;
		res_data[1] = lz * rx - lx * rz;
		res_data[2] = lx * ry - ly * rx;
	}
};

template <class TYPE, class OP, idx_t N>
static void ArrayFixedCombine(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &lstate = state.Cast<ExecuteFunctionState>();
	const auto &expr = lstate.expr.Cast<BoundFunctionExpression>();
	const auto &func_name = expr.function.name;

	const auto count = args.size();
	auto &lhs_child = ArrayVector::GetEntry(args.data[0]);
	auto &rhs_child = ArrayVector::GetEntry(args.data[1]);
	auto &res_child = ArrayVector::GetEntry(result);

	const auto &lhs_child_validity = FlatVector::Validity(lhs_child);
	const auto &rhs_child_validity = FlatVector::Validity(rhs_child);

	UnifiedVectorFormat lhs_format;
	UnifiedVectorFormat rhs_format;

	args.data[0].ToUnifiedFormat(count, lhs_format);
	args.data[1].ToUnifiedFormat(count, rhs_format);

	auto lhs_data = FlatVector::GetData<TYPE>(lhs_child);
	auto rhs_data = FlatVector::GetData<TYPE>(rhs_child);
	auto res_data = FlatVector::GetData<TYPE>(res_child);

	for (idx_t i = 0; i < count; i++) {
		const auto lhs_idx = lhs_format.sel->get_index(i);
		const auto rhs_idx = rhs_format.sel->get_index(i);

		if (!lhs_format.validity.RowIsValid(lhs_idx) || !rhs_format.validity.RowIsValid(rhs_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		const auto left_offset = lhs_idx * N;
		if (!lhs_child_validity.CheckAllValid(left_offset + N, left_offset)) {
			throw InvalidInputException(StringUtil::Format("%s: left argument can not contain NULL values", func_name));
		}

		const auto right_offset = rhs_idx * N;
		if (!rhs_child_validity.CheckAllValid(right_offset + N, right_offset)) {
			throw InvalidInputException(
			    StringUtil::Format("%s: right argument can not contain NULL values", func_name));
		}
		const auto result_offset = i * N;

		const auto lhs_data_ptr = lhs_data + left_offset;
		const auto rhs_data_ptr = rhs_data + right_offset;
		const auto res_data_ptr = res_data + result_offset;

		OP::Operation(lhs_data_ptr, rhs_data_ptr, res_data_ptr, N);
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Generic "fold" function
//------------------------------------------------------------------------------
// Given two arrays, combine and reduce their elements into a single scalar value.

template <class TYPE, class OP>
static void ArrayGenericFold(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &lstate = state.Cast<ExecuteFunctionState>();
	const auto &expr = lstate.expr.Cast<BoundFunctionExpression>();
	const auto &func_name = expr.function.name;

	const auto count = args.size();
	auto &lhs_child = ArrayVector::GetEntry(args.data[0]);
	auto &rhs_child = ArrayVector::GetEntry(args.data[1]);

	const auto &lhs_child_validity = FlatVector::Validity(lhs_child);
	const auto &rhs_child_validity = FlatVector::Validity(rhs_child);

	UnifiedVectorFormat lhs_format;
	UnifiedVectorFormat rhs_format;

	args.data[0].ToUnifiedFormat(count, lhs_format);
	args.data[1].ToUnifiedFormat(count, rhs_format);

	auto lhs_data = FlatVector::GetData<TYPE>(lhs_child);
	auto rhs_data = FlatVector::GetData<TYPE>(rhs_child);
	auto res_data = FlatVector::GetData<TYPE>(result);

	const auto array_size = ArrayType::GetSize(args.data[0].GetType());
	D_ASSERT(array_size == ArrayType::GetSize(args.data[1].GetType()));

	for (idx_t i = 0; i < count; i++) {
		const auto lhs_idx = lhs_format.sel->get_index(i);
		const auto rhs_idx = rhs_format.sel->get_index(i);

		if (!lhs_format.validity.RowIsValid(lhs_idx) || !rhs_format.validity.RowIsValid(rhs_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		const auto left_offset = lhs_idx * array_size;
		if (!lhs_child_validity.CheckAllValid(left_offset + array_size, left_offset)) {
			throw InvalidInputException(StringUtil::Format("%s: left argument can not contain NULL values", func_name));
		}

		const auto right_offset = rhs_idx * array_size;
		if (!rhs_child_validity.CheckAllValid(right_offset + array_size, right_offset)) {
			throw InvalidInputException(
			    StringUtil::Format("%s: right argument can not contain NULL values", func_name));
		}

		const auto lhs_data_ptr = lhs_data + left_offset;
		const auto rhs_data_ptr = rhs_data + right_offset;

		res_data[i] = OP::Operation(lhs_data_ptr, rhs_data_ptr, array_size);
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Function Registration
//------------------------------------------------------------------------------
// Note: In the future we could add a wrapper with a non-type template parameter to specialize for specific array sizes
// e.g. 256, 512, 1024, 2048 etc. which may allow the compiler to vectorize the loop better. Perhaps something for an
// extension.

template <class OP>
static void AddArrayFoldFunction(ScalarFunctionSet &set, const LogicalType &type) {
	const auto array = LogicalType::ARRAY(type, optional_idx());
	if (type.id() == LogicalTypeId::FLOAT) {
		set.AddFunction(ScalarFunction({array, array}, type, ArrayGenericFold<float, OP>, ArrayGenericBinaryBind));
	} else if (type.id() == LogicalTypeId::DOUBLE) {
		set.AddFunction(ScalarFunction({array, array}, type, ArrayGenericFold<double, OP>, ArrayGenericBinaryBind));
	} else {
		throw NotImplementedException("Array function not implemented for type %s", type.ToString());
	}
}

ScalarFunctionSet ArrayDistanceFun::GetFunctions() {
	ScalarFunctionSet set("array_distance");
	for (auto &type : LogicalType::Real()) {
		AddArrayFoldFunction<DistanceOp>(set, type);
	}
	return set;
}

ScalarFunctionSet ArrayInnerProductFun::GetFunctions() {
	ScalarFunctionSet set("array_inner_product");
	for (auto &type : LogicalType::Real()) {
		AddArrayFoldFunction<InnerProductOp>(set, type);
	}
	return set;
}

ScalarFunctionSet ArrayNegativeInnerProductFun::GetFunctions() {
	ScalarFunctionSet set("array_negative_inner_product");
	for (auto &type : LogicalType::Real()) {
		AddArrayFoldFunction<NegativeInnerProductOp>(set, type);
	}
	return set;
}

ScalarFunctionSet ArrayCosineSimilarityFun::GetFunctions() {
	ScalarFunctionSet set("array_cosine_similarity");
	for (auto &type : LogicalType::Real()) {
		AddArrayFoldFunction<CosineSimilarityOp>(set, type);
	}
	return set;
}

ScalarFunctionSet ArrayCosineDistanceFun::GetFunctions() {
	ScalarFunctionSet set("array_cosine_distance");
	for (auto &type : LogicalType::Real()) {
		AddArrayFoldFunction<CosineDistanceOp>(set, type);
	}
	return set;
}

ScalarFunctionSet ArrayCrossProductFun::GetFunctions() {
	ScalarFunctionSet set("array_cross_product");

	auto float_array = LogicalType::ARRAY(LogicalType::FLOAT, 3);
	auto double_array = LogicalType::ARRAY(LogicalType::DOUBLE, 3);
	set.AddFunction(
	    ScalarFunction({float_array, float_array}, float_array, ArrayFixedCombine<float, CrossProductOp, 3>));
	set.AddFunction(
	    ScalarFunction({double_array, double_array}, double_array, ArrayFixedCombine<double, CrossProductOp, 3>));
	return set;
}

} // namespace duckdb
