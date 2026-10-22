#include "duckdb/function/scalar/numeric_dispatch.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/numeric_blob.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/vector/numeric_vector.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/table/numeric_range.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arguments
//===--------------------------------------------------------------------===//
//! A NUMERIC argument in its run-time representation (canonical values are decoded into the representation that holds
//! them); other arguments as they are
static unique_ptr<Vector> TypedArgument(Vector &arg, idx_t count) {
	if (arg.GetType().id() != LogicalTypeId::NUMERIC) {
		return make_uniq<Vector>(Vector::Ref(arg));
	}
	if (arg.GetVectorType() == VectorType::SHREDDED_VECTOR) {
		return make_uniq<Vector>(Vector::Ref(ShreddedVector::GetShreddedVector(arg)));
	}
	if (arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// a constant is decoded once and stays a constant for the kernel
		auto type = NumericBlob::RangeType(arg, 1);
		auto result = make_uniq<Vector>(type, count);
		NumericBlob::Decode(arg, 1, *result);
		result->SetVectorType(VectorType::CONSTANT_VECTOR);
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

//! The range of values an operand takes: the value of a constant, the exact range of the vector's values when it can
//! be known, otherwise everything its representation holds
bool TryGetOperandRange(Vector &arg, Vector &typed, idx_t count, ExactRange &range) {
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
	// the range recorded on a shredded NUMERIC vector, otherwise the values (canonical NUMERIC arguments were decoded)
	ShreddedNumericRange vector_range;
	bool known =
	    (arg.GetType().id() == LogicalTypeId::NUMERIC && NumericVector::TryGetRange(arg, count, vector_range)) ||
	    NumericVector::TryComputeRange(typed, count, vector_range);
	if (known) {
		range.min = vector_range.min;
		range.max = vector_range.max;
		range.scale = vector_range.scale;
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

//! The range of every possible result of the operation over the operand ranges; false beyond 128 bits
bool TryResultRange(NumericDispatchKind kind, vector<ExactRange> ranges, ExactRange &result) {
	if (ranges.size() == 1) {
		result.scale = ranges[0].scale;
		return TryNegate(ranges[0].max, result.min) && TryNegate(ranges[0].min, result.max);
	}
	if (kind == NumericDispatchKind::MULTIPLY) {
		idx_t scale = idx_t(ranges[0].scale) + ranges[1].scale;
		hugeint_t candidates[4];
		if (scale > Decimal::MAX_WIDTH_DECIMAL || !Hugeint::TryMultiply(ranges[0].min, ranges[1].min, candidates[0]) ||
		    !Hugeint::TryMultiply(ranges[0].min, ranges[1].max, candidates[1]) ||
		    !Hugeint::TryMultiply(ranges[0].max, ranges[1].min, candidates[2]) ||
		    !Hugeint::TryMultiply(ranges[0].max, ranges[1].max, candidates[3])) {
			return false;
		}
		result.min = candidates[0];
		result.max = candidates[0];
		for (idx_t i = 1; i < 4; i++) {
			result.min = MinValue(result.min, candidates[i]);
			result.max = MaxValue(result.max, candidates[i]);
		}
		result.scale = NumericCast<uint8_t>(scale);
		return true;
	}
	auto scale = MaxValue(ranges[0].scale, ranges[1].scale);
	if (!AlignScale(ranges[0], scale) || !AlignScale(ranges[1], scale)) {
		return false;
	}
	result.min = ranges[0].min;
	result.max = ranges[0].max;
	result.scale = scale;
	if (kind == NumericDispatchKind::ADD) {
		return Hugeint::TryAddInPlace(result.min, ranges[1].min) && Hugeint::TryAddInPlace(result.max, ranges[1].max);
	}
	return Hugeint::TrySubtractInPlace(result.min, ranges[1].max) &&
	       Hugeint::TrySubtractInPlace(result.max, ranges[1].min);
}

//! The result representation sized by the digits of the operand representations (beyond 128 bits)
LogicalType ResultTypeByDigits(NumericDispatchKind kind, vector<unique_ptr<Vector>> &typed) {
	uint8_t left_digits, left_scale;
	GetOperandDigits(typed[0]->GetType(), left_digits, left_scale);
	if (typed.size() == 1) {
		return NumericType::ForDigits(left_digits, left_scale);
	}
	uint8_t right_digits, right_scale;
	GetOperandDigits(typed[1]->GetType(), right_digits, right_scale);
	if (kind == NumericDispatchKind::MULTIPLY) {
		idx_t scale = idx_t(left_scale) + right_scale;
		if (scale > Decimal::MAX_WIDTH_DECIMAL) {
			throw OutOfRangeException("NUMERIC multiplication needs more than %d digits", Decimal::MAX_WIDTH_DECIMAL);
		}
		return NumericType::ForDigits(idx_t(left_digits) + right_digits, NumericCast<uint8_t>(scale));
	}
	return NumericType::ForDigits(idx_t(MaxValue(left_digits, right_digits)) + 1, MaxValue(left_scale, right_scale));
}

//! The representations the operands are cast to so that the typed operator of the result representation produces
//! every possible result exactly
vector<LogicalType> WidenedKernelTypes(NumericDispatchKind kind, vector<unique_ptr<Vector>> &typed,
                                       const LogicalType &result_type, const vector<ExactRange> &ranges,
                                       bool have_ranges) {
	vector<LogicalType> cast_types;
	if (kind == NumericDispatchKind::MULTIPLY && result_type.id() == LogicalTypeId::DECIMAL) {
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

//! Whether a representation holds every value of a range
bool RepresentationHolds(const LogicalType &type, const ExactRange &range) {
	hugeint_t min, max;
	uint8_t scale;
	if (!NumericType::TryGetRepresentationRange(type, min, max, scale)) {
		return false;
	}
	return scale == range.scale && min <= range.min && range.max <= max;
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
	//! The buffer the kernel writes its result into, reused across calls
	unique_ptr<VectorCache> result_cache;
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

//! An arithmetic kernel is sized so that no result overflows. Its operator is told so through its statistics
//! callback, which replaces it with the variant without overflow checks (as the optimizer does when column
//! statistics prove it).
void DisableOverflowChecks(ClientContext &context, BoundFunctionExpression &expr) {
	auto statistics_callback = expr.Function().GetStatisticsCallback();
	if (!statistics_callback) {
		return;
	}
	vector<BaseStatistics> child_stats;
	for (auto &child : expr.GetChildren()) {
		auto &type = child->GetReturnType();
		if (!NumericType::IsExact(type) || type.InternalType() == PhysicalType::VARCHAR) {
			return;
		}
		auto stats = NumericStats::CreateEmpty(type);
		auto zero = Value::Numeric(type, 0);
		if (type.id() == LogicalTypeId::DECIMAL) {
			auto width = DecimalType::GetWidth(type);
			auto scale = DecimalType::GetScale(type);
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				zero = Value::DECIMAL(int16_t(0), width, scale);
				break;
			case PhysicalType::INT32:
				zero = Value::DECIMAL(int32_t(0), width, scale);
				break;
			case PhysicalType::INT64:
				zero = Value::DECIMAL(int64_t(0), width, scale);
				break;
			default:
				zero = Value::DECIMAL(hugeint_t(0), width, scale);
				break;
			}
		}
		NumericStats::SetMin(stats, zero);
		NumericStats::SetMax(stats, zero);
		child_stats.push_back(std::move(stats));
	}
	FunctionStatisticsInput input(expr, expr.BindInfoMutable().get(), child_stats, nullptr);
	statistics_callback(context, input);
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
		if (is_operator) {
			DisableOverflowChecks(context, kernel->expression->Cast<BoundFunctionExpression>());
		}
		kernel->executor = make_uniq<ExpressionExecutor>(context, *kernel->expression);
		kernel->chunk.InitializeEmpty(types);
		kernel->result_cache =
		    make_uniq<VectorCache>(Allocator::DefaultAllocator(), kernel->expression->GetReturnType());
		kernels.push_back(std::move(kernel));
		return *kernels.back();
	}
};

unique_ptr<FunctionLocalState> InitNumericDispatchState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data) {
	return make_uniq<NumericDispatchState>();
}

//! An exact result becomes a NUMERIC vector that carries the exact range of its values; the representation is the
//! kernel's unless the kernel widened the physical type past the operands and the values fit a narrower one
void FinishResult(const LogicalType &return_type, Vector &typed_result, idx_t count, idx_t operand_size,
                  optional_ptr<const ExactRange> interval, Vector &result) {
	if (return_type.id() == LogicalTypeId::NUMERIC) {
		typed_result.Flatten();
		auto &result_type = typed_result.GetType();
		ShreddedNumericRange range;
		bool known;
		uint8_t result_scale = result_type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(result_type) : 0;
		if (interval && interval->scale == result_scale) {
			// every possible result lies in the interval of the operand ranges
			range.valid = true;
			range.scale = interval->scale;
			range.min = interval->min;
			range.max = interval->max;
			known = true;
		} else {
			known = NumericVector::TryComputeRange(typed_result, count, range);
		}
		if (known && GetTypeIdSize(result_type.InternalType()) > operand_size) {
			auto narrow = NumericType::ForUnscaledRange(range.min, range.max, range.scale);
			if (narrow.InternalType() != result_type.InternalType()) {
				Vector narrowed(narrow, count);
				VectorOperations::DefaultCast(typed_result, narrowed, count, true);
				result.Shred(narrowed, count);
				NumericVector::SetRange(result, range);
				return;
			}
		}
		result.Shred(typed_result, count);
		if (known) {
			NumericVector::SetRange(result, range);
		}
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
	vector<Value> constants(typed.size(), Value(LogicalType(LogicalTypeId::INVALID)));
	idx_t operand_size = 0;
	for (idx_t i = 0; i < typed.size(); i++) {
		if (args.data[i].GetType().id() != LogicalTypeId::NUMERIC &&
		    args.data[i].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			constants[i] = args.data[i].GetValue(0);
		}
		auto &type = typed[i]->GetType();
		if (type.id() != LogicalTypeId::SQLNULL) {
			operand_size = MaxValue<idx_t>(operand_size, GetTypeIdSize(type.InternalType()));
		}
	}
	auto &name = func_expr.Function().GetName();
	bool is_operator = KIND != NumericDispatchKind::GENERIC;

	vector<LogicalType> kernel_types;
	for (auto &operand : typed) {
		kernel_types.push_back(operand->GetType());
	}
	optional_ptr<NumericKernel> kernel;
	ExactRange interval;
	bool resolved = false;
	if (KIND == NumericDispatchKind::GENERIC) {
		kernel = lstate.Get(context, name, kernel_types, constants, is_operator);
	} else {
		// size the kernel from the values the operands take: the operator of the operand representations when its
		// result holds every possible result, otherwise the operands are widened to the result representation
		vector<ExactRange> ranges(typed.size());
		bool have_ranges = true;
		bool have_nulls = false;
		for (idx_t i = 0; i < typed.size(); i++) {
			if (!TryGetOperandRange(args.data[i], *typed[i], count, ranges[i])) {
				have_ranges = false;
			}
			have_nulls = have_nulls || typed[i]->GetType().id() == LogicalTypeId::SQLNULL;
		}
		LogicalType result_type;
		resolved = have_ranges && TryResultRange(KIND, ranges, interval);
		if (resolved) {
			result_type = NumericType::ForUnscaledRange(interval.min, interval.max, interval.scale);
			if (!have_nulls) {
				auto &candidate = lstate.Get(context, name, kernel_types, constants, is_operator);
				if (RepresentationHolds(candidate.expression->GetReturnType(), interval)) {
					kernel = &candidate;
				}
			}
		} else {
			result_type = ResultTypeByDigits(KIND, typed);
		}
		if (!kernel) {
			kernel_types = WidenedKernelTypes(KIND, typed, result_type, ranges, have_ranges);
			kernel = lstate.Get(context, name, kernel_types, constants, is_operator);
		}
	}

	vector<unique_ptr<Vector>> operands;
	for (idx_t i = 0; i < typed.size(); i++) {
		if (typed[i]->GetType() == kernel_types[i]) {
			operands.push_back(std::move(typed[i]));
			continue;
		}
		auto cast = make_uniq<Vector>(kernel_types[i], count);
		VectorOperations::DefaultCast(*typed[i], *cast, count, true);
		operands.push_back(std::move(cast));
	}
	for (idx_t i = 0; i < operands.size(); i++) {
		kernel->chunk.data[i].Reference(*operands[i]);
	}
	kernel->chunk.SetChildCardinality(count);
	Vector typed_result(*kernel->result_cache);
	kernel->executor->ExecuteExpression(kernel->chunk, typed_result);
	FinishResult(func_expr.GetReturnType(), typed_result, count, operand_size, resolved ? &interval : nullptr, result);
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
