#include "duckdb/function/scalar/numeric_dispatch.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/numeric_blob.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/table/numeric_range.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arguments
//===--------------------------------------------------------------------===//
//! A NUMERIC argument in its run-time representation (canonical values are decoded into the representation that holds
//! them); other arguments as they are
static unique_ptr<Vector> TypedArgument(Vector &arg, idx_t count) {
	if (arg.GetType().id() != LogicalTypeId::NUMERIC) {
		auto result = make_uniq<Vector>(arg.GetType());
		result->Reference(arg);
		return result;
	}
	if (arg.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		auto &typed = ShreddedVector::GetShreddedVector(arg);
		auto result = make_uniq<Vector>(typed.GetType());
		result->Reference(typed);
		return result;
	}
	auto type = NumericBlob::RangeType(arg, count);
	auto result = make_uniq<Vector>(type, count);
	NumericBlob::Decode(arg, count, *result);
	return result;
}

//===--------------------------------------------------------------------===//
// Sizing the kernel representation of exact arithmetic
//===--------------------------------------------------------------------===//
namespace {

//! The range of unscaled values an operand can take, as integers at `scale` fractional digits
struct ExactRange {
	hugeint_t min;
	hugeint_t max;
	uint8_t scale = 0;
};

bool AlignScale(ExactRange &range, uint8_t scale) {
	if (range.scale == scale) {
		return true;
	}
	D_ASSERT(scale > range.scale);
	idx_t shift = scale - range.scale;
	if (shift >= Hugeint::CACHED_POWERS_OF_TEN) {
		return false;
	}
	auto factor = Hugeint::POWERS_OF_TEN[shift];
	if (!Hugeint::TryMultiply(range.min, factor, range.min) || !Hugeint::TryMultiply(range.max, factor, range.max)) {
		return false;
	}
	range.scale = scale;
	return true;
}

bool TryGetUnscaled(const Value &value, hugeint_t &result) {
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		result = value.GetValueUnsafe<int8_t>();
		return true;
	case PhysicalType::INT16:
		result = value.GetValueUnsafe<int16_t>();
		return true;
	case PhysicalType::INT32:
		result = value.GetValueUnsafe<int32_t>();
		return true;
	case PhysicalType::INT64:
		result = value.GetValueUnsafe<int64_t>();
		return true;
	case PhysicalType::INT128:
		result = value.GetValueUnsafe<hugeint_t>();
		return true;
	case PhysicalType::UINT8:
		result = value.GetValueUnsafe<uint8_t>();
		return true;
	case PhysicalType::UINT16:
		result = value.GetValueUnsafe<uint16_t>();
		return true;
	case PhysicalType::UINT32:
		result = value.GetValueUnsafe<uint32_t>();
		return true;
	case PhysicalType::UINT64:
		result = hugeint_t(value.GetValueUnsafe<uint64_t>());
		return true;
	default:
		return false;
	}
}

//! The range of values an operand can take: the value of a constant, otherwise everything its representation holds
bool TryGetOperandRange(Vector &typed, ExactRange &range) {
	auto &type = typed.GetType();
	if (type.id() == LogicalTypeId::SQLNULL) {
		range.min = 0;
		range.max = 0;
		range.scale = 0;
		return true;
	}
	if (!NumericType::IsExact(type)) {
		return false;
	}
	if (typed.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto value = typed.GetValue(0);
		range.scale = type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0;
		if (value.IsNull()) {
			range.min = 0;
			range.max = 0;
			return true;
		}
		if (!TryGetUnscaled(value, range.min)) {
			return false;
		}
		range.max = range.min;
		return true;
	}
	return NumericType::TryGetRepresentationRange(type, range.min, range.max, range.scale);
}

void GetOperandDigits(const LogicalType &type, uint8_t &integer_digits, uint8_t &scale) {
	if (type.id() == LogicalTypeId::SQLNULL) {
		integer_digits = 0;
		scale = 0;
		return;
	}
	NumericType::GetDigits(type, integer_digits, scale);
}

bool TryNegate(hugeint_t value, hugeint_t &result) {
	if (value == NumericLimits<hugeint_t>::Minimum()) {
		return false;
	}
	result = -value;
	return true;
}

//! The representations the operands are cast to so that the typed operator produces every possible result exactly
vector<LogicalType> ExactKernelTypes(NumericDispatchKind kind, vector<unique_ptr<Vector>> &typed) {
	bool is_multiply = kind == NumericDispatchKind::MULTIPLY;
	bool is_add = kind == NumericDispatchKind::ADD;
	vector<ExactRange> ranges(typed.size());
	bool have_ranges = true;
	for (idx_t i = 0; i < typed.size(); i++) {
		if (!TryGetOperandRange(*typed[i], ranges[i])) {
			have_ranges = false;
		}
	}
	LogicalType result_type;
	bool resolved = false;
	if (have_ranges) {
		if (typed.size() == 1) {
			ExactRange result;
			result.scale = ranges[0].scale;
			if (TryNegate(ranges[0].max, result.min) && TryNegate(ranges[0].min, result.max)) {
				result_type = NumericType::ForUnscaledRange(result.min, result.max, result.scale);
				resolved = true;
			}
		} else if (is_multiply) {
			idx_t scale = idx_t(ranges[0].scale) + ranges[1].scale;
			hugeint_t candidates[4];
			if (scale <= Decimal::MAX_WIDTH_DECIMAL &&
			    Hugeint::TryMultiply(ranges[0].min, ranges[1].min, candidates[0]) &&
			    Hugeint::TryMultiply(ranges[0].min, ranges[1].max, candidates[1]) &&
			    Hugeint::TryMultiply(ranges[0].max, ranges[1].min, candidates[2]) &&
			    Hugeint::TryMultiply(ranges[0].max, ranges[1].max, candidates[3])) {
				auto min = candidates[0];
				auto max = candidates[0];
				for (idx_t i = 1; i < 4; i++) {
					min = MinValue(min, candidates[i]);
					max = MaxValue(max, candidates[i]);
				}
				result_type = NumericType::ForUnscaledRange(min, max, NumericCast<uint8_t>(scale));
				resolved = true;
			}
		} else {
			auto scale = MaxValue(ranges[0].scale, ranges[1].scale);
			if (AlignScale(ranges[0], scale) && AlignScale(ranges[1], scale)) {
				hugeint_t min = ranges[0].min;
				hugeint_t max = ranges[0].max;
				bool ok;
				if (is_add) {
					ok = Hugeint::TryAddInPlace(min, ranges[1].min) && Hugeint::TryAddInPlace(max, ranges[1].max);
				} else {
					ok = Hugeint::TrySubtractInPlace(min, ranges[1].max) &&
					     Hugeint::TrySubtractInPlace(max, ranges[1].min);
				}
				if (ok) {
					result_type = NumericType::ForUnscaledRange(min, max, scale);
					resolved = true;
				}
			}
		}
	}
	if (!resolved) {
		// beyond 128 bits: size the result by digits
		uint8_t left_digits, left_scale;
		GetOperandDigits(typed[0]->GetType(), left_digits, left_scale);
		if (typed.size() == 1) {
			result_type = NumericType::ForDigits(left_digits, left_scale);
		} else {
			uint8_t right_digits, right_scale;
			GetOperandDigits(typed[1]->GetType(), right_digits, right_scale);
			if (is_multiply) {
				idx_t scale = idx_t(left_scale) + right_scale;
				if (scale > Decimal::MAX_WIDTH_DECIMAL) {
					throw OutOfRangeException("NUMERIC multiplication needs more than %d digits",
					                          Decimal::MAX_WIDTH_DECIMAL);
				}
				result_type = NumericType::ForDigits(idx_t(left_digits) + right_digits, NumericCast<uint8_t>(scale));
			} else {
				result_type = NumericType::ForDigits(idx_t(MaxValue(left_digits, right_digits)) + 1,
				                                     MaxValue(left_scale, right_scale));
			}
		}
	}

	vector<LogicalType> cast_types;
	if (is_multiply && result_type.id() == LogicalTypeId::DECIMAL) {
		// the decimal multiplication keeps the scales of its operands and adds their widths: widen the first operand so
		// the sum of the widths covers the result, and lift it past the fixed-size tiers when the result needs that
		auto result_width = DecimalType::GetWidth(result_type);
		uint8_t widths[2], scales[2];
		for (idx_t i = 0; i < 2; i++) {
			auto &type = typed[i]->GetType();
			if (type.id() == LogicalTypeId::SQLNULL) {
				widths[i] = 1;
				scales[i] = 0;
			} else if (have_ranges) {
				scales[i] = ranges[i].scale;
				widths[i] = NumericCast<uint8_t>(MaxValue<idx_t>(
				    MaxValue(NumericType::Digits(ranges[i].min), NumericType::Digits(ranges[i].max)), scales[i]));
			} else {
				NumericType::GetDigits(type, widths[i], scales[i]);
				widths[i] = NumericCast<uint8_t>(widths[i] + scales[i]);
			}
		}
		idx_t left_width = MaxValue<idx_t>(widths[0], result_width > widths[1] ? result_width - widths[1] : 0);
		if (result_width > Decimal::MAX_WIDTH_INT128 && left_width <= Decimal::MAX_WIDTH_INT128 &&
		    widths[1] <= Decimal::MAX_WIDTH_INT128) {
			left_width = Decimal::MAX_WIDTH_INT128 + 1;
		} else if (result_width > Decimal::MAX_WIDTH_INT64 && left_width <= Decimal::MAX_WIDTH_INT64 &&
		           widths[1] <= Decimal::MAX_WIDTH_INT64) {
			left_width = Decimal::MAX_WIDTH_INT64 + 1;
		}
		left_width = MaxValue<idx_t>(left_width, scales[0]);
		if (left_width > Decimal::MAX_WIDTH_DECIMAL) {
			throw OutOfRangeException("NUMERIC multiplication needs more than %d digits", Decimal::MAX_WIDTH_DECIMAL);
		}
		cast_types.push_back(LogicalType::DECIMAL(NumericCast<uint8_t>(left_width), scales[0]));
		cast_types.push_back(LogicalType::DECIMAL(MaxValue(widths[1], scales[1]), scales[1]));
	} else {
		for (idx_t i = 0; i < typed.size(); i++) {
			cast_types.push_back(result_type);
		}
	}
	return cast_types;
}

//===--------------------------------------------------------------------===//
// The typed kernels, bound per signature
//===--------------------------------------------------------------------===//
struct NumericKernel {
	vector<LogicalType> types;
	//! Non-NUMERIC constant arguments are bound as constants (e.g. the precision of round must be one)
	vector<Value> constants;
	unique_ptr<Expression> expression;
	unique_ptr<ExpressionExecutor> executor;
	DataChunk chunk;
};

//! Whether two sets of constant arguments are the same (placeholders for non-constant arguments are NULL)
static bool SameConstants(const vector<Value> &left, const vector<Value> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (left[i].type() != right[i].type()) {
			return false;
		}
		if (left[i].IsNull() || right[i].IsNull()) {
			if (left[i].IsNull() != right[i].IsNull()) {
				return false;
			}
			continue;
		}
		if (!ValueOperations::NotDistinctFrom(left[i], right[i])) {
			return false;
		}
	}
	return true;
}

struct NumericDispatchState : public FunctionLocalState {
	vector<unique_ptr<NumericKernel>> kernels;

	NumericKernel &Get(ClientContext &context, const Identifier &name, const vector<LogicalType> &types,
	                   const vector<Value> &constants, bool is_operator) {
		for (auto &kernel : kernels) {
			if (kernel->types == types && SameConstants(kernel->constants, constants)) {
				return *kernel;
			}
		}
		auto kernel = make_uniq<NumericKernel>();
		kernel->types = types;
		kernel->constants = constants;
		vector<unique_ptr<Expression>> children;
		for (idx_t i = 0; i < types.size(); i++) {
			if (constants[i].type().id() != LogicalTypeId::INVALID) {
				children.push_back(make_uniq<BoundConstantExpression>(constants[i]));
			} else {
				children.push_back(make_uniq<BoundReferenceExpression>(types[i], i));
			}
		}
		FunctionBinder binder(context);
		ErrorData error;
		kernel->expression =
		    binder.BindScalarFunction(Identifier(DEFAULT_SCHEMA), name, std::move(children), error, is_operator);
		if (!kernel->expression) {
			error.Throw();
		}
		kernel->executor = make_uniq<ExpressionExecutor>(context, *kernel->expression);
		kernel->chunk.InitializeEmpty(types);
		kernels.push_back(std::move(kernel));
		return *kernels.back();
	}
};

unique_ptr<FunctionLocalState> InitNumericDispatchState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data) {
	return make_uniq<NumericDispatchState>();
}

//! An exact result becomes a NUMERIC vector in the narrowest representation that holds its values
void FinishResult(const LogicalType &return_type, Vector &typed_result, idx_t count, Vector &result) {
	if (return_type.id() == LogicalTypeId::NUMERIC) {
		typed_result.Flatten(count);
		NumericRange range;
		range.Update(typed_result, count);
		auto narrow = range.ResolveType();
		if (narrow != typed_result.GetType()) {
			Vector narrowed(narrow, count);
			VectorOperations::DefaultCast(typed_result, narrowed, count, true);
			result.Shred(narrowed, count);
			return;
		}
		result.Shred(typed_result, count);
		return;
	}
	if (typed_result.GetType() == return_type) {
		result.Reference(typed_result);
		return;
	}
	VectorOperations::DefaultCast(typed_result, result, count, true);
}

template <NumericDispatchKind KIND>
void NumericDispatchFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<NumericDispatchState>();
	idx_t count = args.size();

	vector<unique_ptr<Vector>> typed;
	for (auto &arg : args.data) {
		typed.push_back(TypedArgument(arg, count));
	}
	vector<LogicalType> kernel_types;
	if (KIND == NumericDispatchKind::GENERIC) {
		for (auto &operand : typed) {
			kernel_types.push_back(operand->GetType());
		}
	} else {
		kernel_types = ExactKernelTypes(KIND, typed);
	}
	vector<unique_ptr<Vector>> operands;
	vector<Value> constants(typed.size(), Value(LogicalType(LogicalTypeId::INVALID)));
	for (idx_t i = 0; i < typed.size(); i++) {
		if (args.data[i].GetType().id() != LogicalTypeId::NUMERIC &&
		    args.data[i].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			constants[i] = args.data[i].GetValue(0);
		}
		if (typed[i]->GetType() == kernel_types[i]) {
			operands.push_back(std::move(typed[i]));
			continue;
		}
		auto cast = make_uniq<Vector>(kernel_types[i], count);
		VectorOperations::DefaultCast(*typed[i], *cast, count, true);
		operands.push_back(std::move(cast));
	}

	auto &kernel = lstate.Get(context, func_expr.Function().GetName(), kernel_types, constants,
	                          KIND != NumericDispatchKind::GENERIC);
	for (idx_t i = 0; i < operands.size(); i++) {
		kernel.chunk.data[i].Reference(*operands[i]);
	}
	kernel.chunk.SetCardinality(count);
	Vector typed_result(kernel.expression->GetReturnType(), count);
	kernel.executor->ExecuteExpression(kernel.chunk, typed_result);
	FinishResult(func_expr.GetReturnType(), typed_result, count, result);
}

//! The run-time representation of a NUMERIC vector
void NumericRepresentationFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	string representation = input.GetVectorType() == VectorType::SHREDDED_VECTOR
	                            ? ShreddedVector::GetShreddedVector(input).GetType().ToString()
	                            : "CANONICAL";
	Vector constant(Value(representation), count_t(1));
	result.Reference(constant);
}

} // namespace

ScalarFunction NumericDispatch::GetFunction(const string &name, vector<LogicalType> arguments, LogicalType return_type,
                                            NumericDispatchKind kind) {
	scalar_function_t function;
	switch (kind) {
	case NumericDispatchKind::ADD:
		function = NumericDispatchFunction<NumericDispatchKind::ADD>;
		break;
	case NumericDispatchKind::SUBTRACT:
		function = NumericDispatchFunction<NumericDispatchKind::SUBTRACT>;
		break;
	case NumericDispatchKind::MULTIPLY:
		function = NumericDispatchFunction<NumericDispatchKind::MULTIPLY>;
		break;
	default:
		function = NumericDispatchFunction<NumericDispatchKind::GENERIC>;
		break;
	}
	ScalarFunction result(Identifier(name), std::move(arguments), std::move(return_type), function,
	                      bind_scalar_function_t(nullptr), function_statistics_t(nullptr), InitNumericDispatchState);
	result.SetFallible();
	return result;
}

ScalarFunction NumericRepresentationFun::GetFunction() {
	return ScalarFunction({LogicalType::NUMERIC}, LogicalType::VARCHAR, NumericRepresentationFunction);
}

} // namespace duckdb
