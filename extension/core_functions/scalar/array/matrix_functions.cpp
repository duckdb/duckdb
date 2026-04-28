#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "core_functions/scalar/array_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/value.hpp"
#include <limits>

namespace duckdb {

//------------------------------------------------------------------------------
// Helper: Extract shape (list of dimension sizes) from a nested ARRAY type
//------------------------------------------------------------------------------
static vector<idx_t> ExtractShape(const LogicalType &type) {
	vector<idx_t> shape;
	auto current = type;
	while (current.id() == LogicalTypeId::ARRAY) {
		shape.push_back(ArrayType::GetSize(current));
		current = ArrayType::GetChildType(current);
	}
	return shape;
}

//------------------------------------------------------------------------------
// Helper: Get the innermost (leaf) element type of a nested ARRAY
//------------------------------------------------------------------------------
static LogicalType GetLeafType(const LogicalType &type) {
	auto current = type;
	while (current.id() == LogicalTypeId::ARRAY) {
		current = ArrayType::GetChildType(current);
	}
	return current;
}

//------------------------------------------------------------------------------
// Helper: Validate input is a 2D array and extract dimensions
//------------------------------------------------------------------------------
static void Validate2DArray(const string &func_name, const LogicalType &type, idx_t &rows, idx_t &cols) {
	if (type.id() != LogicalTypeId::ARRAY) {
		throw BinderException("%s: Argument must be a 2D array (matrix)", func_name);
	}
	auto &child_type = ArrayType::GetChildType(type);
	if (child_type.id() != LogicalTypeId::ARRAY) {
		throw BinderException("%s: Argument must be a 2D array (matrix), got 1D array", func_name);
	}
	auto &leaf_type = ArrayType::GetChildType(child_type);
	if (leaf_type.id() == LogicalTypeId::ARRAY) {
		throw BinderException("%s: Argument must be a 2D array (matrix), got 3D+ array", func_name);
	}
	rows = ArrayType::GetSize(type);
	cols = ArrayType::GetSize(child_type);
}

//------------------------------------------------------------------------------
// Helper: Validate element type is numeric (FLOAT, DOUBLE, INTEGER, BIGINT)
//------------------------------------------------------------------------------
static LogicalType ValidateNumericElementType(const string &func_name, const LogicalType &leaf_type) {
	switch (leaf_type.id()) {
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return leaf_type;
	default:
		throw BinderException("%s: Array elements must be FLOAT, DOUBLE, INTEGER, or BIGINT, got %s", func_name,
		                      leaf_type.ToString());
	}
}

//==============================================================================
// array_tensor_shape
//==============================================================================

struct TensorShapeBindData : public FunctionData {
	vector<idx_t> shape;

	explicit TensorShapeBindData(vector<idx_t> shape_p) : shape(std::move(shape_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TensorShapeBindData>(shape);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TensorShapeBindData>();
		return shape == other.shape;
	}
};

static unique_ptr<FunctionData> TensorShapeBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &input_type = arguments[0]->return_type;

	if (input_type.id() != LogicalTypeId::ARRAY) {
		throw BinderException("array_tensor_shape: Argument must be an array type");
	}

	auto shape = ExtractShape(input_type);
	bound_function.SetReturnType(LogicalType::LIST(LogicalType::INTEGER));
	return make_uniq<TensorShapeBindData>(std::move(shape));
}

static void TensorShapeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.Cast<ExecuteFunctionState>().expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<TensorShapeBindData>();
	auto count = args.size();
	auto &shape = bind_data.shape;

	UnifiedVectorFormat input_format;
	args.data[0].ToUnifiedFormat(count, input_format);

	auto ndims = shape.size();

	// Reserve before grabbing child pointers — Reserve may reallocate the child buffer.
	ListVector::Reserve(result, count * ndims);
	ListVector::SetListSize(result, count * ndims);

	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto &child_vector = ListVector::GetChildMutable(result);
	auto child_data = FlatVector::GetDataMutable<int32_t>(child_vector);

	for (idx_t i = 0; i < count; i++) {
		auto idx = input_format.sel->get_index(i);
		if (!input_format.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		list_entries[i].offset = i * NumericCast<idx_t>(ndims);
		list_entries[i].length = NumericCast<idx_t>(ndims);
		for (idx_t d = 0; d < ndims; d++) {
			child_data[i * ndims + d] = NumericCast<int32_t>(shape[d]);
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction ArrayTensorShapeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::LIST(LogicalType::INTEGER), TensorShapeFunction,
	                          TensorShapeBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

//==============================================================================
// array_transpose
//==============================================================================

struct TransposeBindData : public FunctionData {
	idx_t rows;
	idx_t cols;

	TransposeBindData(idx_t rows, idx_t cols) : rows(rows), cols(cols) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TransposeBindData>(rows, cols);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TransposeBindData>();
		return rows == other.rows && cols == other.cols;
	}
};

static unique_ptr<FunctionData> TransposeBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &input_type = arguments[0]->return_type;

	idx_t rows, cols;
	Validate2DArray("array_transpose", input_type, rows, cols);

	auto leaf_type = GetLeafType(input_type);
	// Transpose: T[m][n] -> T[n][m]
	auto result_type = LogicalType::ARRAY(LogicalType::ARRAY(leaf_type, rows), cols);
	bound_function.GetArguments()[0] = input_type;
	bound_function.SetReturnType(result_type);
	return make_uniq<TransposeBindData>(rows, cols);
}

template <class TYPE>
static void TransposeExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.Cast<ExecuteFunctionState>().expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<TransposeBindData>();
	auto &func_name = func_expr.function.name;

	auto rows = bind_data.rows;
	auto cols = bind_data.cols;
	auto count = args.size();

	args.data[0].Flatten(count);
	auto &mid_in = ArrayVector::GetChildMutable(args.data[0]);
	auto &inner_in = ArrayVector::GetChildMutable(mid_in);
	auto in_data = FlatVector::GetData<TYPE>(inner_in);
	auto &in_validity = FlatVector::Validity(args.data[0]);
	auto &in_mid_validity = FlatVector::Validity(mid_in);
	auto &in_inner_validity = FlatVector::Validity(inner_in);

	auto &mid_out = ArrayVector::GetChildMutable(result);
	auto &inner_out = ArrayVector::GetChildMutable(mid_out);
	auto out_data = FlatVector::GetDataMutable<TYPE>(inner_out);

	auto total = rows * cols;

	for (idx_t i = 0; i < count; i++) {
		if (!in_validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto in_offset = i * total;
		if (!in_mid_validity.CheckAllValid(i * rows + rows, i * rows)) {
			throw InvalidInputException("%s: input matrix rows cannot be NULL", func_name);
		}
		if (!in_inner_validity.CheckAllValid(in_offset + total, in_offset)) {
			throw InvalidInputException("%s: input matrix elements cannot be NULL", func_name);
		}

		auto out_offset = i * total;
		for (idx_t r = 0; r < rows; r++) {
			for (idx_t c = 0; c < cols; c++) {
				out_data[out_offset + c * rows + r] = in_data[in_offset + r * cols + c];
			}
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ArrayTransposeFun::GetFunctions() {
	ScalarFunctionSet set("array_transpose");

	auto make_2d = [](const LogicalType &elem) {
		return LogicalType::ARRAY(LogicalType::ARRAY(elem, optional_idx()), optional_idx());
	};

	{
		ScalarFunction func({make_2d(LogicalType::FLOAT)}, LogicalType::ANY, TransposeExecute<float>, TransposeBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		ScalarFunction func({make_2d(LogicalType::DOUBLE)}, LogicalType::ANY, TransposeExecute<double>, TransposeBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		ScalarFunction func({make_2d(LogicalType::INTEGER)}, LogicalType::ANY, TransposeExecute<int32_t>,
		                    TransposeBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		ScalarFunction func({make_2d(LogicalType::BIGINT)}, LogicalType::ANY, TransposeExecute<int64_t>, TransposeBind);
		func.SetFallible();
		set.AddFunction(func);
	}

	return set;
}

//==============================================================================
// array_flatten
//==============================================================================

struct FlattenBindData : public FunctionData {
	idx_t total_elements;

	explicit FlattenBindData(idx_t total) : total_elements(total) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<FlattenBindData>(total_elements);
	}

	bool Equals(const FunctionData &other_p) const override {
		return total_elements == other_p.Cast<FlattenBindData>().total_elements;
	}
};

static unique_ptr<FunctionData> ArrayFlattenBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &input_type = arguments[0]->return_type;

	if (input_type.id() != LogicalTypeId::ARRAY) {
		throw BinderException("array_flatten: Argument must be an array type");
	}

	auto shape = ExtractShape(input_type);
	idx_t total = 1;
	for (auto d : shape) {
		total *= d;
	}

	auto leaf_type = GetLeafType(input_type);
	bound_function.GetArguments()[0] = input_type;
	bound_function.SetReturnType(LogicalType::ARRAY(leaf_type, total));
	return make_uniq<FlattenBindData>(total);
}

static void ArrayFlattenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.Cast<ExecuteFunctionState>().expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<FlattenBindData>();
	auto &func_name = func_expr.function.name;
	auto count = args.size();
	auto total = bind_data.total_elements;

	args.data[0].Flatten(count);
	auto &in_validity = FlatVector::Validity(args.data[0]);

	// Validate no mid-level NULLs exist for non-NULL rows
	Vector *current = &args.data[0];
	idx_t elements_per_row = 1;
	while (ArrayType::GetChildType(current->GetType()).id() == LogicalTypeId::ARRAY) {
		auto array_size = ArrayType::GetSize(current->GetType());
		elements_per_row *= array_size;
		auto &child = ArrayVector::GetChildMutable(*current);
		auto &child_validity = FlatVector::Validity(child);
		for (idx_t i = 0; i < count; i++) {
			if (!in_validity.RowIsValid(i)) {
				continue;
			}
			auto offset = i * elements_per_row;
			if (!child_validity.CheckAllValid(offset + elements_per_row, offset)) {
				throw InvalidInputException("%s: nested array elements cannot be NULL", func_name);
			}
		}
		current = &child;
	}
	auto &leaf_data = ArrayVector::GetChildMutable(*current);

	// Copy flat data to result
	auto &result_child = ArrayVector::GetChildMutable(result);
	VectorOperations::Copy(leaf_data, result_child, count * total, 0, 0);

	for (idx_t i = 0; i < count; i++) {
		if (!in_validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction ArrayFlattenFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ARRAY(LogicalType::ANY, optional_idx())}, LogicalType::ANY,
	                          ArrayFlattenFunction, ArrayFlattenBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

//==============================================================================
// array_reshape
//==============================================================================

struct ReshapeBindData : public FunctionData {
	vector<idx_t> target_shape;
	idx_t total_elements;

	ReshapeBindData(vector<idx_t> shape, idx_t total) : target_shape(std::move(shape)), total_elements(total) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ReshapeBindData>(target_shape, total_elements);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReshapeBindData>();
		return target_shape == other.target_shape && total_elements == other.total_elements;
	}
};

static unique_ptr<FunctionData> ArrayReshapeBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &input_type = arguments[0]->return_type;

	if (input_type.id() != LogicalTypeId::ARRAY) {
		throw BinderException("array_reshape: First argument must be an array type");
	}

	if (arguments.size() < 2) {
		throw BinderException("array_reshape: At least one dimension argument is required");
	}

	// Extract dimension constants
	vector<idx_t> target_shape;
	for (idx_t i = 1; i < arguments.size(); i++) {
		if (arguments[i]->HasParameter()) {
			throw BinderException("array_reshape: Dimension arguments must be constant integers, not parameters");
		}
		if (!arguments[i]->IsFoldable()) {
			throw BinderException("array_reshape: Dimension arguments must be constant integers");
		}
		auto val = ExpressionExecutor::EvaluateScalar(input.GetClientContext(), *arguments[i]);
		if (val.IsNull()) {
			throw BinderException("array_reshape: Dimension arguments cannot be NULL");
		}
		auto dim = val.GetValue<int64_t>();
		if (dim <= 0) {
			throw BinderException("array_reshape: Dimension arguments must be positive integers, got %lld", dim);
		}
		target_shape.push_back(NumericCast<idx_t>(dim));
	}

	// Compute total elements from source
	auto source_shape = ExtractShape(input_type);
	idx_t source_total = 1;
	for (auto d : source_shape) {
		source_total *= d;
	}

	// Compute total from target
	idx_t target_total = 1;
	for (auto d : target_shape) {
		target_total *= d;
	}

	if (source_total != target_total) {
		throw BinderException(
		    "array_reshape: Total elements mismatch: source has %llu elements, target shape requires %llu",
		    source_total, target_total);
	}

	// Build result type from shape (innermost dimension last)
	auto leaf_type = GetLeafType(input_type);
	auto result_type = leaf_type;
	for (int i = NumericCast<int>(target_shape.size()) - 1; i >= 0; i--) {
		result_type = LogicalType::ARRAY(result_type, target_shape[i]);
	}

	bound_function.GetArguments()[0] = input_type;
	bound_function.SetReturnType(result_type);
	return make_uniq<ReshapeBindData>(std::move(target_shape), source_total);
}

static void ArrayReshapeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.Cast<ExecuteFunctionState>().expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<ReshapeBindData>();
	auto &func_name = func_expr.function.name;
	auto count = args.size();
	auto total = bind_data.total_elements;

	args.data[0].Flatten(count);
	auto &in_validity = FlatVector::Validity(args.data[0]);

	// Navigate to innermost source data, validating no mid-level NULLs
	Vector *src = &args.data[0];
	idx_t elements_per_row = 1;
	while (ArrayType::GetChildType(src->GetType()).id() == LogicalTypeId::ARRAY) {
		auto array_size = ArrayType::GetSize(src->GetType());
		elements_per_row *= array_size;
		auto &child = ArrayVector::GetChildMutable(*src);
		auto &child_validity = FlatVector::Validity(child);
		for (idx_t i = 0; i < count; i++) {
			if (!in_validity.RowIsValid(i)) {
				continue;
			}
			auto offset = i * elements_per_row;
			if (!child_validity.CheckAllValid(offset + elements_per_row, offset)) {
				throw InvalidInputException("%s: nested array elements cannot be NULL", func_name);
			}
		}
		src = &child;
	}
	auto &src_leaf = ArrayVector::GetChildMutable(*src);

	// Navigate to innermost result data
	Vector *dst = &result;
	while (ArrayType::GetChildType(dst->GetType()).id() == LogicalTypeId::ARRAY) {
		dst = &ArrayVector::GetChildMutable(*dst);
	}
	auto &dst_leaf = ArrayVector::GetChildMutable(*dst);

	VectorOperations::Copy(src_leaf, dst_leaf, count * total, 0, 0);

	for (idx_t i = 0; i < count; i++) {
		if (!in_validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction ArrayReshapeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ARRAY(LogicalType::ANY, optional_idx())}, LogicalType::ANY,
	                          ArrayReshapeFunction, ArrayReshapeBind);
	fun.SetVarArgs(LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

//==============================================================================
// array_matmul
//==============================================================================

struct MatmulBindData : public FunctionData {
	idx_t m; // rows of A / result
	idx_t k; // cols of A / rows of B
	idx_t n; // cols of B / result

	MatmulBindData(idx_t m, idx_t k, idx_t n) : m(m), k(k), n(n) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<MatmulBindData>(m, k, n);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MatmulBindData>();
		return m == other.m && k == other.k && n == other.n;
	}
};

static unique_ptr<FunctionData> MatmulBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &lhs_type = arguments[0]->return_type;
	auto &rhs_type = arguments[1]->return_type;

	idx_t m, k1, k2, n;
	Validate2DArray("array_matmul", lhs_type, m, k1);
	Validate2DArray("array_matmul", rhs_type, k2, n);

	if (k1 != k2) {
		throw BinderException("array_matmul: Inner dimensions must match: A is [%llu][%llu], B is [%llu][%llu]", m, k1,
		                      k2, n);
	}

	auto lhs_leaf = GetLeafType(lhs_type);
	auto rhs_leaf = GetLeafType(rhs_type);

	LogicalType common_type;
	if (!LogicalType::TryGetMaxLogicalType(context, lhs_leaf, rhs_leaf, common_type)) {
		throw BinderException("array_matmul: Cannot infer common element type (left = '%s', right = '%s')",
		                      lhs_leaf.ToString(), rhs_leaf.ToString());
	}
	common_type = ValidateNumericElementType("array_matmul", common_type);

	auto result_type = LogicalType::ARRAY(LogicalType::ARRAY(common_type, n), m);
	bound_function.GetArguments()[0] = LogicalType::ARRAY(LogicalType::ARRAY(common_type, k1), m);
	bound_function.GetArguments()[1] = LogicalType::ARRAY(LogicalType::ARRAY(common_type, n), k2);
	bound_function.SetReturnType(result_type);
	return make_uniq<MatmulBindData>(m, k1, n);
}

template <class TYPE>
static void MatmulExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.Cast<ExecuteFunctionState>().expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<MatmulBindData>();
	auto &func_name = func_expr.function.name;

	auto m = bind_data.m;
	auto k = bind_data.k;
	auto n = bind_data.n;
	auto count = args.size();

	// Flatten inputs
	args.data[0].Flatten(count);
	args.data[1].Flatten(count);

	auto &a_mid = ArrayVector::GetChildMutable(args.data[0]);
	auto &a_inner = ArrayVector::GetChildMutable(a_mid);
	auto a_data = FlatVector::GetData<TYPE>(a_inner);
	auto &a_validity = FlatVector::Validity(args.data[0]);
	auto &a_mid_validity = FlatVector::Validity(a_mid);
	auto &a_inner_validity = FlatVector::Validity(a_inner);

	auto &b_mid = ArrayVector::GetChildMutable(args.data[1]);
	auto &b_inner = ArrayVector::GetChildMutable(b_mid);
	auto b_data = FlatVector::GetData<TYPE>(b_inner);
	auto &b_validity = FlatVector::Validity(args.data[1]);
	auto &b_mid_validity = FlatVector::Validity(b_mid);
	auto &b_inner_validity = FlatVector::Validity(b_inner);

	auto &res_mid = ArrayVector::GetChildMutable(result);
	auto &res_inner = ArrayVector::GetChildMutable(res_mid);
	auto res_data = FlatVector::GetDataMutable<TYPE>(res_inner);

	auto a_total = m * k;
	auto b_total = k * n;
	auto r_total = m * n;

	for (idx_t i = 0; i < count; i++) {
		if (!a_validity.RowIsValid(i) || !b_validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto a_offset = i * a_total;
		auto b_offset = i * b_total;
		auto r_offset = i * r_total;

		// Check for NULL rows/elements
		if (!a_mid_validity.CheckAllValid(i * m + m, i * m)) {
			throw InvalidInputException("%s: left matrix rows cannot be NULL", func_name);
		}
		if (!a_inner_validity.CheckAllValid(a_offset + a_total, a_offset)) {
			throw InvalidInputException("%s: left matrix elements cannot be NULL", func_name);
		}
		if (!b_mid_validity.CheckAllValid(i * k + k, i * k)) {
			throw InvalidInputException("%s: right matrix rows cannot be NULL", func_name);
		}
		if (!b_inner_validity.CheckAllValid(b_offset + b_total, b_offset)) {
			throw InvalidInputException("%s: right matrix elements cannot be NULL", func_name);
		}

		// Matrix multiply: C[r][c] = sum_j A[r][j] * B[j][c]
		auto a_ptr = a_data + a_offset;
		auto b_ptr = b_data + b_offset;
		auto r_ptr = res_data + r_offset;

		for (idx_t r = 0; r < m; r++) {
			for (idx_t c = 0; c < n; c++) {
				TYPE sum = 0;
				for (idx_t j = 0; j < k; j++) {
					sum += a_ptr[r * k + j] * b_ptr[j * n + c];
				}
				r_ptr[r * n + c] = sum;
			}
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ArrayMatmulFun::GetFunctions() {
	ScalarFunctionSet set("array_matmul");

	auto make_2d = [](const LogicalType &elem) {
		return LogicalType::ARRAY(LogicalType::ARRAY(elem, optional_idx()), optional_idx());
	};

	{
		auto t = make_2d(LogicalType::FLOAT);
		ScalarFunction func({t, t}, LogicalType::ANY, MatmulExecute<float>, MatmulBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		auto t = make_2d(LogicalType::DOUBLE);
		ScalarFunction func({t, t}, LogicalType::ANY, MatmulExecute<double>, MatmulBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		auto t = make_2d(LogicalType::INTEGER);
		ScalarFunction func({t, t}, LogicalType::ANY, MatmulExecute<int32_t>, MatmulBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		auto t = make_2d(LogicalType::BIGINT);
		ScalarFunction func({t, t}, LogicalType::ANY, MatmulExecute<int64_t>, MatmulBind);
		func.SetFallible();
		set.AddFunction(func);
	}

	return set;
}

//==============================================================================
// array_trace
//==============================================================================

struct TraceBindData : public FunctionData {
	idx_t n;

	explicit TraceBindData(idx_t n) : n(n) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TraceBindData>(n);
	}

	bool Equals(const FunctionData &other_p) const override {
		return n == other_p.Cast<TraceBindData>().n;
	}
};

static unique_ptr<FunctionData> TraceBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &input_type = arguments[0]->return_type;

	idx_t rows, cols;
	Validate2DArray("array_trace", input_type, rows, cols);

	if (rows != cols) {
		throw BinderException("array_trace: Matrix must be square, got [%llu][%llu]", rows, cols);
	}

	auto leaf_type = GetLeafType(input_type);
	auto element_type = ValidateNumericElementType("array_trace", leaf_type);

	bound_function.GetArguments()[0] = input_type;
	bound_function.SetReturnType(element_type);
	return make_uniq<TraceBindData>(rows);
}

template <class TYPE>
static void TraceExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.Cast<ExecuteFunctionState>().expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<TraceBindData>();
	auto &func_name = func_expr.function.name;

	auto n = bind_data.n;
	auto count = args.size();
	auto total = n * n;

	args.data[0].Flatten(count);
	auto &mid = ArrayVector::GetChildMutable(args.data[0]);
	auto &inner = ArrayVector::GetChildMutable(mid);
	auto in_data = FlatVector::GetData<TYPE>(inner);
	auto &in_validity = FlatVector::Validity(args.data[0]);
	auto &in_inner_validity = FlatVector::Validity(inner);

	auto res_data = FlatVector::GetDataMutable<TYPE>(result);

	for (idx_t i = 0; i < count; i++) {
		if (!in_validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto offset = i * total;
		if (!in_inner_validity.CheckAllValid(offset + total, offset)) {
			throw InvalidInputException("%s: matrix elements cannot be NULL", func_name);
		}

		TYPE trace = 0;
		for (idx_t d = 0; d < n; d++) {
			trace += in_data[offset + d * n + d];
		}
		res_data[i] = trace;
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ArrayTraceFun::GetFunctions() {
	ScalarFunctionSet set("array_trace");

	auto make_2d = [](const LogicalType &elem) {
		return LogicalType::ARRAY(LogicalType::ARRAY(elem, optional_idx()), optional_idx());
	};

	{
		ScalarFunction func({make_2d(LogicalType::FLOAT)}, LogicalType::ANY, TraceExecute<float>, TraceBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		ScalarFunction func({make_2d(LogicalType::DOUBLE)}, LogicalType::ANY, TraceExecute<double>, TraceBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		ScalarFunction func({make_2d(LogicalType::INTEGER)}, LogicalType::ANY, TraceExecute<int32_t>, TraceBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		ScalarFunction func({make_2d(LogicalType::BIGINT)}, LogicalType::ANY, TraceExecute<int64_t>, TraceBind);
		func.SetFallible();
		set.AddFunction(func);
	}

	return set;
}

//==============================================================================
// array_determinant
//==============================================================================

struct DeterminantBindData : public FunctionData {
	idx_t n;

	explicit DeterminantBindData(idx_t n) : n(n) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DeterminantBindData>(n);
	}

	bool Equals(const FunctionData &other_p) const override {
		return n == other_p.Cast<DeterminantBindData>().n;
	}
};

static unique_ptr<FunctionData> DeterminantBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &input_type = arguments[0]->return_type;

	idx_t rows, cols;
	Validate2DArray("array_determinant", input_type, rows, cols);

	if (rows != cols) {
		throw BinderException("array_determinant: Matrix must be square, got [%llu][%llu]", rows, cols);
	}

	auto leaf_type = GetLeafType(input_type);
	auto element_type = ValidateNumericElementType("array_determinant", leaf_type);

	bound_function.GetArguments()[0] = input_type;
	bound_function.SetReturnType(element_type);
	return make_uniq<DeterminantBindData>(rows);
}

template <class TYPE>
static TYPE Determinant2x2(const TYPE *data) {
	return data[0] * data[3] - data[1] * data[2];
}

template <class TYPE>
static TYPE Determinant3x3(const TYPE *data) {
	return data[0] * (data[4] * data[8] - data[5] * data[7]) - data[1] * (data[3] * data[8] - data[5] * data[6]) +
	       data[2] * (data[3] * data[7] - data[4] * data[6]);
}

template <class TYPE>
static TYPE DeterminantLU(const TYPE *data, idx_t n) {
	// LU decomposition with partial pivoting
	vector<TYPE> lu(n * n);
	TYPE max_abs = 0;
	for (idx_t i = 0; i < n * n; i++) {
		lu[i] = data[i];
		max_abs = MaxValue(max_abs, AbsValue(data[i]));
	}

	// Scale singularity threshold by matrix magnitude so it works
	// correctly for both very large and very small element values
	auto tolerance = max_abs * std::numeric_limits<TYPE>::epsilon();

	TYPE det = TYPE(1);
	for (idx_t col = 0; col < n; col++) {
		// Find pivot
		idx_t max_row = col;
		TYPE max_val = AbsValue(lu[col * n + col]);
		for (idx_t row = col + 1; row < n; row++) {
			TYPE val = AbsValue(lu[row * n + col]);
			if (val > max_val) {
				max_val = val;
				max_row = row;
			}
		}

		if (max_val < tolerance) {
			return TYPE(0); // Singular (or near-singular) matrix
		}

		// Swap rows
		if (max_row != col) {
			for (idx_t j = 0; j < n; j++) {
				std::swap(lu[col * n + j], lu[max_row * n + j]);
			}
			det = -det; // Row swap flips sign
		}

		det *= lu[col * n + col];

		// Eliminate below
		for (idx_t row = col + 1; row < n; row++) {
			TYPE factor = lu[row * n + col] / lu[col * n + col];
			for (idx_t j = col + 1; j < n; j++) {
				lu[row * n + j] -= factor * lu[col * n + j];
			}
		}
	}

	return det;
}

template <class TYPE>
static void DeterminantExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.Cast<ExecuteFunctionState>().expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<DeterminantBindData>();
	auto &func_name = func_expr.function.name;

	auto n = bind_data.n;
	auto count = args.size();
	auto total = n * n;

	args.data[0].Flatten(count);
	auto &mid = ArrayVector::GetChildMutable(args.data[0]);
	auto &inner = ArrayVector::GetChildMutable(mid);
	auto in_data = FlatVector::GetData<TYPE>(inner);
	auto &in_validity = FlatVector::Validity(args.data[0]);
	auto &in_inner_validity = FlatVector::Validity(inner);

	auto res_data = FlatVector::GetDataMutable<TYPE>(result);

	for (idx_t i = 0; i < count; i++) {
		if (!in_validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto offset = i * total;
		if (!in_inner_validity.CheckAllValid(offset + total, offset)) {
			throw InvalidInputException("%s: matrix elements cannot be NULL", func_name);
		}

		auto data_ptr = in_data + offset;
		if (n == 1) {
			res_data[i] = data_ptr[0];
		} else if (n == 2) {
			res_data[i] = Determinant2x2<TYPE>(data_ptr);
		} else if (n == 3) {
			res_data[i] = Determinant3x3<TYPE>(data_ptr);
		} else {
			res_data[i] = DeterminantLU<TYPE>(data_ptr, n);
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ArrayDeterminantFun::GetFunctions() {
	ScalarFunctionSet set("array_determinant");

	auto make_2d = [](const LogicalType &elem) {
		return LogicalType::ARRAY(LogicalType::ARRAY(elem, optional_idx()), optional_idx());
	};

	{
		ScalarFunction func({make_2d(LogicalType::FLOAT)}, LogicalType::ANY, DeterminantExecute<float>,
		                    DeterminantBind);
		func.SetFallible();
		set.AddFunction(func);
	}
	{
		ScalarFunction func({make_2d(LogicalType::DOUBLE)}, LogicalType::ANY, DeterminantExecute<double>,
		                    DeterminantBind);
		func.SetFallible();
		set.AddFunction(func);
	}

	return set;
}

} // namespace duckdb
