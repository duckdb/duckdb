#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector/for_vector_arithmetic.hpp"
#include "duckdb/function/scalar/operator_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

namespace {

// decimal scale-up casts multiply the payload by 10^delta: run them narrow when the FOR bounds allow it
template <class DST_T>
bool TryFORScaleUp(Vector &source, Vector &result, idx_t count, uint8_t dst_width, uint64_t mul,
                   buffer_ptr<DictionaryEntry> &dict_cache) {
#ifdef DUCKDB_SMALLER_BINARY
	(void)source;
	(void)result;
	(void)count;
	(void)dst_width;
	(void)mul;
	(void)dict_cache;
	return false;
#else
	FORVector::ScanData<DST_T> scan;
	if (!FORVector::TryGetScanData(source, scan)) {
		return false;
	}
	DST_T result_max;
	if (!TryMultiplyOperator::Operation(scan.max_value, DST_T(mul), result_max)) {
		return false;
	}
	if (FORValueOps<DST_T>::ToUnsignedStorage(result_max) >= Uhugeint::POWERS_OF_TEN[dst_width]) {
		return false;
	}
	PhysicalType res_stored;
	if (!FORVector::TryGetStoredTypeForMax<DST_T>(result_max, res_stored)) {
		return false;
	}
	using EXECUTOR = FORStandardExecutor<MultiplyOperator>;
	const auto compute = EXECUTOR::MaxStored(res_stored, scan.stored_type);
	auto fill = [&](Vector &target, idx_t n) {
		FORVector::Create<DST_T>(target, compute, result_max);
		uint64_t cval = mul;
		BinaryBufferArgs args;
		args.ldata = FORVector::GetData(*scan.for_vec);
		args.lvalidity = scan.validity.get();
		args.rdata = const_data_ptr_cast(&cval);
		args.rconstant = true;
		args.result_data = FORVector::GetData(target);
		args.result_validity = &FORVector::Validity(target);
		args.count = n;
		EXECUTOR::Kernel(scan.stored_type, compute, compute)(args);
	};
	if (scan.sel) {
		const idx_t child_count = scan.for_vec->size();
		if (child_count > STANDARD_VECTOR_SIZE) {
			return false;
		}
		if (!dict_cache || dict_cache->data.GetType() != result.GetType()) {
			dict_cache = make_buffer<DictionaryEntry>(Vector(result.GetType(), STANDARD_VECTOR_SIZE));
			// full-stride allocation and pipeline-local, so consumers may widen it in place
			dict_cache->data.BufferMutable().cache_owned = true;
		}
		fill(dict_cache->data, child_count);
		result.Dictionary(dict_cache, *scan.sel, count);
	} else {
		fill(result, count);
	}
	return true;
#endif
}

bool TryFORDecimalScaleUp(Vector &source, Vector &result, idx_t count, buffer_ptr<DictionaryEntry> &dict_cache) {
	auto &src_type = source.GetType();
	auto &dst_type = result.GetType();
	if (src_type.id() != LogicalTypeId::DECIMAL || dst_type.id() != LogicalTypeId::DECIMAL ||
	    GetTypeIdSize(dst_type.InternalType()) < GetTypeIdSize(src_type.InternalType())) {
		return false;
	}
	const auto src_scale = DecimalType::GetScale(src_type);
	const auto dst_scale = DecimalType::GetScale(dst_type);
	if (dst_scale <= src_scale || dst_scale - src_scale >= 20) {
		return false;
	}
	uint64_t mul = 1;
	for (auto d = src_scale; d < dst_scale; d++) {
		mul *= 10;
	}
	bool ok = false;
	FOR_SWITCH_LOGICAL(dst_type.InternalType(), DST_T, {
		ok = TryFORScaleUp<DST_T>(source, result, count, DecimalType::GetWidth(dst_type), mul, dict_cache);
	});
	return ok;
}

} // namespace

static BoundCastInfo BindCastScalarFunction(ClientContext &context, const LogicalType &source,
                                            const LogicalType &target) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput input(context);
	return cast_functions.GetCastFunction(source, target, input);
}

//! Casts to UNION types must run even for constant NULL inputs (the union member tag is set even for NULL values),
//! so we disable the executor's constant-NULL short-circuit for those. For all other targets casts propagate NULLs
//! normally, and using default NULL handling keeps Expression::PropagatesNullValues() accurate for the optimizer.
template <class FUNC>
static void SetCastNullHandling(FUNC &function, const LogicalType &target_type) {
	if (target_type.id() == LogicalTypeId::UNION) {
		function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	}
}

struct CastFunctionData : public FunctionData {
	CastFunctionData(LogicalType source_type_p, LogicalType target_type_p, BoundCastInfo bound_cast_p, bool try_cast_p)
	    : source_type(std::move(source_type_p)), target_type(std::move(target_type_p)),
	      bound_cast(std::move(bound_cast_p)), try_cast(try_cast_p) {
	}

	LogicalType source_type;
	LogicalType target_type;
	BoundCastInfo bound_cast;
	bool try_cast;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CastFunctionData>(source_type, target_type, bound_cast.Copy(), try_cast);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CastFunctionData>();
		return source_type == other.source_type && target_type == other.target_type && try_cast == other.try_cast;
	}
};

void CastFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &cast_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &cast_data = cast_expr.BindInfo()->Cast<CastFunctionData>();
	auto lstate = ExecuteFunctionState::GetFunctionState(state);

	auto &source = args.data[0];
	idx_t count = args.size();

	if (FORVector::TryCastType(source, result, count) ||
	    TryFORDecimalScaleUp(source, result, count, state.Cast<ExecuteFunctionState>().for_dictionary)) {
		// FOR-preserving cast: re-typed or rescaled over a narrow payload, cannot fail
		if (result.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
			FlatVector::SetSize(result, count_t(count));
		}
		return;
	}

	// Constant inputs are handled by the generic function executor (cardinality is reduced to 1). NULL handling is
	// marked SPECIAL_HANDLING so that constant NULL inputs still reach the cast (required for e.g. UNION targets).
	string error_message;
	auto error_ref = cast_data.try_cast ? &error_message : nullptr;
	CastParameters parameters(cast_data.bound_cast.GetCastData(), false, error_ref, lstate);
	parameters.query_location = cast_expr.GetQueryLocation();
	parameters.cast_source = &BoundCastExpression::Child(cast_expr);
	parameters.cast_target = cast_expr;
	cast_data.bound_cast.Cast(source, result, count, parameters);
}

static unique_ptr<FunctionLocalState> CastInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                         FunctionData *bind_data) {
	auto &cast_data = bind_data->Cast<CastFunctionData>();
	if (!cast_data.bound_cast.HasInitLocalState()) {
		return nullptr;
	}
	auto context_ptr = state.root.executor->HasContext() ? &state.root.executor->GetContext() : nullptr;
	CastLocalStateParameters parameters(context_ptr, cast_data.bound_cast.GetCastData());
	return cast_data.bound_cast.InitLocalState(parameters);
}

static unique_ptr<FunctionData> BindCastFun(BindScalarFunctionInput &input) {
	throw InvalidInputException("Cast function cannot be called directly");
}

static string CastToString(FunctionToStringInput &input) {
	auto &cast_data = input.bind_data->Cast<CastFunctionData>();
	string prefix = cast_data.try_cast ? "TRY_CAST(" : "CAST(";
	return prefix + input.GetChild(0).GetName() + " AS " + cast_data.target_type.ToString() + ")";
}

static ExpressionType CastGetExpressionType(FunctionToStringInput &input) {
	return ExpressionType::OPERATOR_CAST;
}

static unique_ptr<Expression> CastLegacySerializeCallback(FunctionToStringInput &input) {
	auto &cast_data = input.bind_data->Cast<CastFunctionData>();
	return make_uniq<LegacyBoundCastExpression>(input.GetChild(0).Copy(), cast_data.target_type, cast_data.try_cast);
}

static void CastFunctionSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                  const BoundScalarFunction &function) {
	auto &bind_data = bind_data_p->Cast<CastFunctionData>();
	serializer.WriteProperty(100, "try_cast", bind_data.try_cast);
}

static unique_ptr<FunctionData> CastFunctionDeserialize(Deserializer &deserializer, BoundScalarFunction &function) {
	auto try_cast = deserializer.ReadProperty<bool>(100, "try_cast");
	auto &context = deserializer.Get<ClientContext &>();
	// the target type is the return type of the function (set by the function serializer)
	auto target_type = deserializer.Get<const LogicalType &>();
	auto source_type = function.GetArguments()[0];
	auto bound_cast = BindCastScalarFunction(context, source_type, target_type);
	if (BoundCastExpression::CastCanThrow(source_type, target_type, try_cast)) {
		function.SetErrorMode(FunctionErrors::CAN_THROW_RUNTIME_ERROR);
	}
	SetCastNullHandling(function, target_type);
	return make_uniq<CastFunctionData>(source_type, target_type, std::move(bound_cast), try_cast);
}

ScalarFunction CastFun::GetFunction() {
	ScalarFunction cast_fun("__cast", {LogicalType::ANY}, LogicalType::ANY, CastFunction, BindCastFun);
	cast_fun.SetToStringCallback(CastToString);
	cast_fun.SetGetExpressionTypeCallback(CastGetExpressionType);
	cast_fun.SetLegacySerializeCallback(CastLegacySerializeCallback);
	cast_fun.SetSerializeCallback(CastFunctionSerialize);
	cast_fun.SetDeserializeCallback(CastFunctionDeserialize);
	cast_fun.SetInitStateCallback(CastInitLocalState);
	return cast_fun;
}

//===--------------------------------------------------------------------===//
// BoundCastExpression
//===--------------------------------------------------------------------===//
unique_ptr<Expression> BoundCastExpression::Create(unique_ptr<Expression> child, const LogicalType &target_type,
                                                   BoundCastInfo bound_cast, bool try_cast) {
	auto source_type = child->GetReturnType();
	auto query_location = child->GetQueryLocation();

	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(child));

	auto function_data = make_uniq<CastFunctionData>(source_type, target_type, std::move(bound_cast), try_cast);

	auto scalar_function = CastFun::GetFunction();
	scalar_function.SetReturnType(target_type);
	if (BoundCastExpression::CastCanThrow(source_type, target_type, try_cast)) {
		scalar_function.SetErrorMode(FunctionErrors::CAN_THROW_RUNTIME_ERROR);
	}
	SetCastNullHandling(scalar_function, target_type);

	BoundScalarFunction bound_function(scalar_function);
	bound_function.GetArguments() = {source_type};

	auto result = make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(children),
	                                                 std::move(function_data), true);
	result->SetQueryLocation(query_location);
	return std::move(result);
}

bool BoundCastExpression::IsCast(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	       expr.GetExpressionType() == ExpressionType::OPERATOR_CAST;
}

const Expression &BoundCastExpression::Child(const BoundFunctionExpression &cast_expr) {
	return *cast_expr.GetChildren()[0];
}

unique_ptr<Expression> &BoundCastExpression::ChildMutable(BoundFunctionExpression &cast_expr) {
	return cast_expr.GetChildrenMutable()[0];
}

const LogicalType &BoundCastExpression::TargetType(const BoundFunctionExpression &cast_expr) {
	return cast_expr.GetReturnType();
}

LogicalType BoundCastExpression::SourceType(const BoundFunctionExpression &cast_expr) {
	return Child(cast_expr).GetReturnType();
}

bool BoundCastExpression::IsTryCast(const BoundFunctionExpression &cast_expr) {
	return cast_expr.BindInfo()->Cast<CastFunctionData>().try_cast;
}

const BoundCastInfo &BoundCastExpression::GetBoundCast(const BoundFunctionExpression &cast_expr) {
	return cast_expr.BindInfo()->Cast<CastFunctionData>().bound_cast;
}

BoundCastInfo &BoundCastExpression::GetBoundCastMutable(BoundFunctionExpression &cast_expr) {
	return cast_expr.BindInfoMutable()->Cast<CastFunctionData>().bound_cast;
}

} // namespace duckdb
