#include "duckdb/core_functions/scalar/array_functions.hpp"
#include <cmath>

namespace duckdb {

//------------------------------------------------------------------------------
// Functors
//------------------------------------------------------------------------------

struct InnerProductOp {
	static constexpr const char *NAME = "array_inner_product";

	template <class TYPE>
	inline static TYPE *GetResultData(Vector &result_vec) {
		return FlatVector::GetData<TYPE>(result_vec);
	}

	template <class TYPE>
	inline static void Operation(TYPE *l_data, idx_t l_idx, TYPE *r_data, idx_t r_idx, TYPE *result_data,
	                             idx_t result_idx, idx_t size) {

		TYPE inner_product = 0;

		auto l_ptr = l_data + (l_idx * size);
		auto r_ptr = r_data + (r_idx * size);

		for (idx_t elem_idx = 0; elem_idx < size; elem_idx++) {
			auto x = *l_ptr++;
			auto y = *r_ptr++;
			inner_product += x * y;
		}

		result_data[result_idx] = inner_product;
	}
};

struct DistanceOp {
	static constexpr const char *NAME = "array_distance";

	template <class TYPE>
	inline static TYPE *GetResultData(Vector &result_vec) {
		return FlatVector::GetData<TYPE>(result_vec);
	}

	template <class TYPE>
	inline static void Operation(TYPE *l_data, idx_t l_idx, TYPE *r_data, idx_t r_idx, TYPE *result_data,
	                             idx_t result_idx, idx_t size) {

		TYPE distance = 0;

		auto l_ptr = l_data + (l_idx * size);
		auto r_ptr = r_data + (r_idx * size);

		for (idx_t elem_idx = 0; elem_idx < size; elem_idx++) {
			auto x = *l_ptr++;
			auto y = *r_ptr++;
			auto diff = x - y;
			distance += diff * diff;
		}

		result_data[result_idx] = std::sqrt(distance);
	}
};

struct CosineSimilarityOp {
	static constexpr const char *NAME = "array_cosine_similarity";

	template <class TYPE>
	inline static TYPE *GetResultData(Vector &result_vec) {
		return FlatVector::GetData<TYPE>(result_vec);
	}

	template <class TYPE>
	inline static void Operation(TYPE *l_data, idx_t l_idx, TYPE *r_data, idx_t r_idx, TYPE *result_data,
	                             idx_t result_idx, idx_t size) {

		TYPE distance = 0;
		TYPE norm_l = 0;
		TYPE norm_r = 0;

		auto l_ptr = l_data + (l_idx * size);
		auto r_ptr = r_data + (r_idx * size);

		for (idx_t i = 0; i < size; i++) {
			auto x = *l_ptr++;
			auto y = *r_ptr++;
			distance += x * y;
			norm_l += x * x;
			norm_r += y * y;
		}

		auto similarity = distance / (std::sqrt(norm_l) * std::sqrt(norm_r));

		// clamp to [-1, 1] to avoid floating point errors
		result_data[result_idx] = std::max(static_cast<TYPE>(-1), std::min(similarity, static_cast<TYPE>(1)));
	}
};

struct CrossProductOp {
	static constexpr const char *NAME = "array_cross_product";

	template <class TYPE>
	inline static TYPE *GetResultData(Vector &result_vec) {
		// Since we return an array here, we need to get the data pointer of the child
		auto &child = ArrayVector::GetEntry(result_vec);
		return FlatVector::GetData<TYPE>(child);
	}

	template <class TYPE>
	inline static void Operation(TYPE *l_data, idx_t l_idx, TYPE *r_data, idx_t r_idx, TYPE *result_data,
	                             idx_t result_idx, idx_t size) {
		D_ASSERT(size == 3);

		auto l_child_idx = l_idx * size;
		auto r_child_idx = r_idx * size;
		auto res_child_idx = result_idx * size;

		auto lx = l_data[l_child_idx + 0];
		auto ly = l_data[l_child_idx + 1];
		auto lz = l_data[l_child_idx + 2];

		auto rx = r_data[r_child_idx + 0];
		auto ry = r_data[r_child_idx + 1];
		auto rz = r_data[r_child_idx + 2];

		result_data[res_child_idx + 0] = ly * rz - lz * ry;
		result_data[res_child_idx + 1] = lz * rx - lx * rz;
		result_data[res_child_idx + 2] = lx * ry - ly * rx;
	}
};

//------------------------------------------------------------------------------
// Generic Execute and Bind
//------------------------------------------------------------------------------
// This is a generic executor function for fast binary math operations on
// real-valued arrays. Array elements are assumed to be either FLOAT or DOUBLE,
// and cannot be null. (although the array itself can be null).
// In the future we could extend this further to be truly generic and handle
// other types, unary/ternary operations and/or nulls.

template <class OP, class TYPE>
static inline void ArrayGenericBinaryExecute(Vector &left, Vector &right, Vector &result, idx_t size, idx_t count) {

	auto &left_child = ArrayVector::GetEntry(left);
	auto &right_child = ArrayVector::GetEntry(right);

	auto &left_child_validity = FlatVector::Validity(left_child);
	auto &right_child_validity = FlatVector::Validity(right_child);

	UnifiedVectorFormat left_format;
	UnifiedVectorFormat right_format;

	left.ToUnifiedFormat(count, left_format);
	right.ToUnifiedFormat(count, right_format);

	auto left_data = FlatVector::GetData<TYPE>(left_child);
	auto right_data = FlatVector::GetData<TYPE>(right_child);
	auto result_data = OP::template GetResultData<TYPE>(result);

	for (idx_t i = 0; i < count; i++) {
		auto left_idx = left_format.sel->get_index(i);
		auto right_idx = right_format.sel->get_index(i);

		if (!left_format.validity.RowIsValid(left_idx) || !right_format.validity.RowIsValid(right_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto left_offset = left_idx * size;
		if (!left_child_validity.CheckAllValid(left_offset + size, left_offset)) {
			throw InvalidInputException(StringUtil::Format("%s: left argument can not contain NULL values", OP::NAME));
		}

		auto right_offset = right_idx * size;
		if (!right_child_validity.CheckAllValid(right_offset + size, right_offset)) {
			throw InvalidInputException(StringUtil::Format("%s: right argument can not contain NULL values", OP::NAME));
		}

		OP::template Operation<TYPE>(left_data, left_idx, right_data, right_idx, result_data, i, size);
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template <class OP>
static void ArrayGenericBinaryFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto size = ArrayType::GetSize(args.data[0].GetType());
	auto child_type = ArrayType::GetChildType(args.data[0].GetType());
	switch (child_type.id()) {
	case LogicalTypeId::DOUBLE:
		ArrayGenericBinaryExecute<OP, double>(args.data[0], args.data[1], result, size, args.size());
		break;
	case LogicalTypeId::FLOAT:
		ArrayGenericBinaryExecute<OP, float>(args.data[0], args.data[1], result, size, args.size());
		break;
	default:
		throw NotImplementedException(StringUtil::Format("%s: Unsupported element type", OP::NAME));
	}
}

template <class OP>
static unique_ptr<FunctionData> ArrayGenericBinaryBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {

	// construct return type
	auto &left_type = arguments[0]->return_type;
	auto &right_type = arguments[1]->return_type;

	// mystery to me how anything non-array could ever end up here but it happened
	if (left_type.id() != LogicalTypeId::ARRAY || right_type.id() != LogicalTypeId::ARRAY) {
		throw InvalidInputException(StringUtil::Format("%s: Arguments must be arrays of FLOAT or DOUBLE", OP::NAME));
	}

	auto left_size = ArrayType::GetSize(left_type);
	auto right_size = ArrayType::GetSize(right_type);
	if (left_size != right_size) {
		throw InvalidInputException(StringUtil::Format("%s: Array arguments must be of the same size", OP::NAME));
	}
	auto size = left_size;

	auto child_type =
	    LogicalType::MaxLogicalType(context, ArrayType::GetChildType(left_type), ArrayType::GetChildType(right_type));
	if (child_type != LogicalTypeId::FLOAT && child_type != LogicalTypeId::DOUBLE) {
		throw InvalidInputException(
		    StringUtil::Format("%s: Array arguments must be of type FLOAT or DOUBLE", OP::NAME));
	}

	// the important part here is that we resolve the array size
	auto array_type = LogicalType::ARRAY(child_type, size);

	bound_function.arguments[0] = array_type;
	bound_function.arguments[1] = array_type;
	bound_function.return_type = child_type;

	return nullptr;
}

template <class OP, class TYPE, idx_t N>
static inline void ArrayFixedBinaryFunction(DataChunk &args, ExpressionState &, Vector &result) {
	ArrayGenericBinaryExecute<OP, TYPE>(args.data[0], args.data[1], result, N, args.size());
}

//------------------------------------------------------------------------------
// Function Registration
//------------------------------------------------------------------------------

// Note: In the future we could add a wrapper with a non-type template parameter to specialize for specific array sizes
// e.g. 256, 512, 1024, 2048 etc. which may allow the compiler to vectorize the loop better. Perhaps something for an
// extension.

ScalarFunctionSet ArrayInnerProductFun::GetFunctions() {
	ScalarFunctionSet set("array_inner_product");
	// Generic array inner product function
	for (auto &type : LogicalType::Real()) {
		set.AddFunction(
		    ScalarFunction({LogicalType::ARRAY(type, optional_idx()), LogicalType::ARRAY(type, optional_idx())}, type,
		                   ArrayGenericBinaryFunction<InnerProductOp>, ArrayGenericBinaryBind<InnerProductOp>));
	}
	return set;
}

ScalarFunctionSet ArrayDistanceFun::GetFunctions() {
	ScalarFunctionSet set("array_distance");
	// Generic array distance function
	for (auto &type : LogicalType::Real()) {
		set.AddFunction(
		    ScalarFunction({LogicalType::ARRAY(type, optional_idx()), LogicalType::ARRAY(type, optional_idx())}, type,
		                   ArrayGenericBinaryFunction<DistanceOp>, ArrayGenericBinaryBind<DistanceOp>));
	}
	return set;
}

ScalarFunctionSet ArrayCosineSimilarityFun::GetFunctions() {
	ScalarFunctionSet set("array_cosine_similarity");
	// Generic array cosine similarity function
	for (auto &type : LogicalType::Real()) {
		set.AddFunction(
		    ScalarFunction({LogicalType::ARRAY(type, optional_idx()), LogicalType::ARRAY(type, optional_idx())}, type,
		                   ArrayGenericBinaryFunction<CosineSimilarityOp>, ArrayGenericBinaryBind<CosineSimilarityOp>));
	}
	return set;
}

ScalarFunctionSet ArrayCrossProductFun::GetFunctions() {
	ScalarFunctionSet set("array_cross_product");

	// Generic array cross product function
	auto double_arr = LogicalType::ARRAY(LogicalType::DOUBLE, 3);
	set.AddFunction(
	    ScalarFunction({double_arr, double_arr}, double_arr, ArrayFixedBinaryFunction<CrossProductOp, double, 3>));

	auto float_arr = LogicalType::ARRAY(LogicalType::FLOAT, 3);
	set.AddFunction(
	    ScalarFunction({float_arr, float_arr}, float_arr, ArrayFixedBinaryFunction<CrossProductOp, float, 3>));
	return set;
}

} // namespace duckdb
