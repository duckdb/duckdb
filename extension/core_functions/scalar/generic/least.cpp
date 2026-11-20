#include "duckdb/common/types/decimal.hpp"
#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

namespace {
struct LeastOp {
	using OP = LessThan;

	static OrderByNullType NullOrdering() {
		return OrderByNullType::NULLS_LAST;
	}
};

struct GreaterOp {
	using OP = GreaterThan;

	static OrderByNullType NullOrdering() {
		return OrderByNullType::NULLS_FIRST;
	}
};

template <class OP>
struct LeastOperator {
	template <class T>
	static T Operation(T left, T right) {
		return OP::Operation(left, right) ? left : right;
	}
};

struct LeastGreatestSortKeyState : public FunctionLocalState {
	explicit LeastGreatestSortKeyState(idx_t column_count, OrderByNullType null_ordering)
	    : intermediate(LogicalType::BLOB), modifiers(OrderType::ASCENDING, null_ordering) {
		vector<LogicalType> types;
		// initialize sort key chunk
		for (idx_t i = 0; i < column_count; i++) {
			types.push_back(LogicalType::BLOB);
		}
		sort_keys.Initialize(Allocator::DefaultAllocator(), types);
	}

	DataChunk sort_keys;
	Vector intermediate;
	OrderModifiers modifiers;
};

template <class OP>
unique_ptr<FunctionLocalState> LeastGreatestSortKeyInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        optional_ptr<const FunctionData> bind_data) {
	return make_uniq<LeastGreatestSortKeyState>(expr.GetChildren().size(), OP::NullOrdering());
}

template <bool STRING>
struct StandardLeastGreatest {
	static constexpr bool IS_STRING = STRING;

	static DataChunk &Prepare(DataChunk &args, ExpressionState &) {
		return args;
	}

	static Vector &TargetVector(Vector &result, ExpressionState &) {
		return result;
	}

	static void FinalizeResult(idx_t rows, bool result_has_value[], Vector &result, ExpressionState &) {
		auto &result_mask = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < rows; i++) {
			if (!result_has_value[i]) {
				result_mask.SetInvalid(i);
			}
		}
	}
};

struct SortKeyLeastGreatest {
	static constexpr bool IS_STRING = false;

	static DataChunk &Prepare(DataChunk &args, ExpressionState &state) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LeastGreatestSortKeyState>();
		lstate.sort_keys.Reset();
		for (idx_t c_idx = 0; c_idx < args.ColumnCount(); c_idx++) {
			CreateSortKeyHelpers::CreateSortKey(args.data[c_idx], lstate.modifiers, lstate.sort_keys.data[c_idx]);
		}
		return lstate.sort_keys;
	}

	static Vector &TargetVector(Vector &result, ExpressionState &state) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LeastGreatestSortKeyState>();
		return lstate.intermediate;
	}

	static void FinalizeResult(idx_t rows, bool result_has_value[], Vector &result, ExpressionState &state) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LeastGreatestSortKeyState>();
		auto result_keys = FlatVector::GetData<string_t>(lstate.intermediate);
		auto &result_mask = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < rows; i++) {
			if (!result_has_value[i]) {
				result_mask.SetInvalid(i);
			} else {
				CreateSortKeyHelpers::DecodeSortKey(result_keys[i], result, i, lstate.modifiers);
			}
		}
	}
};

template <class T, class OP, class BASE_OP = StandardLeastGreatest<false>>
void LeastGreatestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 1) {
		// single input: nop
		result.Reference(args.data[0]);
		return;
	}
	auto &input = BASE_OP::Prepare(args, state);
	auto &result_vector = BASE_OP::TargetVector(result, state);

	auto result_type = VectorType::CONSTANT_VECTOR;
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		if (args.data[col_idx].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			// non-constant input: result is not a constant vector
			result_type = VectorType::FLAT_VECTOR;
		}
		if (BASE_OP::IS_STRING) {
			// for string vectors we add a reference to the heap of the children
			StringVector::AddHeapReference(result_vector, input.data[col_idx]);
		}
	}

	auto result_data = FlatVector::ScatterWriter<T>(result_vector);
	bool result_has_value[STANDARD_VECTOR_SIZE] {false};
	// perform the operation column-by-column
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		if (input.data[col_idx].GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    ConstantVector::IsNull(input.data[col_idx])) {
			// ignore null vector
			continue;
		}

		auto entries = input.data[col_idx].template Values<T>();

		if (entries.CanHaveNull()) {
			// potential new null entries: have to check the null mask
			for (idx_t i = 0; i < input.size(); i++) {
				auto entry = entries[i];
				if (entry.IsValid()) {
					// not a null entry: perform the operation and add to new set
					auto ivalue = entry.GetValue();
					if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
						result_has_value[i] = true;
						result_data[i] = ivalue;
					}
				}
			}
		} else {
			// no new null entries: only need to perform the operation
			for (idx_t i = 0; i < input.size(); i++) {
				auto ivalue = entries.GetValueUnsafe(i);
				if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
					result_has_value[i] = true;
					result_data[i] = ivalue;
				}
			}
		}
	}
	BASE_OP::FinalizeResult(input.size(), result_has_value, result, state);
	result.SetVectorType(result_type);
}

struct LeastGreatestCollationData : public FunctionData {
	explicit LeastGreatestCollationData(unique_ptr<Expression> collation_p) : collation(std::move(collation_p)) {
	}

	//! The collation of the argument type, applied to a reference to column 0
	unique_ptr<Expression> collation;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<LeastGreatestCollationData>(collation->Copy());
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LeastGreatestCollationData>();
		return collation->Equals(*other.collation);
	}
};

struct LeastGreatestCollationState : public FunctionLocalState {
	LeastGreatestCollationState(Context &context, const LogicalType &input_type, const Expression &collation,
	                            idx_t column_count)
	    : executor(context, collation), winner_keys(LogicalType::BLOB), winners(STANDARD_VECTOR_SIZE),
	      modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST) {
		collation_input.InitializeEmpty({input_type});
		collated.Initialize(Allocator::DefaultAllocator(), {collation.GetReturnType()});
		vector<LogicalType> types(column_count, LogicalType::BLOB);
		collation_keys.Initialize(Allocator::DefaultAllocator(), types);
		value_keys.Initialize(Allocator::DefaultAllocator(), types);
	}

	ExpressionExecutor executor;
	DataChunk collation_input;
	DataChunk collated;
	//! Sort keys of the collated arguments, which decide the result
	DataChunk collation_keys;
	//! Sort keys of the arguments, from which the result is decoded
	DataChunk value_keys;
	Vector winner_keys;
	//! The argument each row takes its result from, INVALID_INDEX if all arguments are NULL
	vector<idx_t> winners;
	//! NULL arguments are skipped before comparing, so the NULL ordering is irrelevant
	OrderModifiers modifiers;
};

unique_ptr<FunctionLocalState> LeastGreatestCollationInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                          optional_ptr<const FunctionData> bind_data) {
	auto &children = expr.GetChildren();
	auto &collation = *bind_data->Cast<LeastGreatestCollationData>().collation;
	return make_uniq<LeastGreatestCollationState>(state.GetContext(), children[0]->GetReturnType(), collation,
	                                              children.size());
}

//! Compares the collated arguments, but returns the original argument: a collation can map distinct values to the
//! same key (e.g. INTERVAL 1 MONTH and INTERVAL 30 DAY)
template <class OP>
void LeastGreatestCollationFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 1) {
		result.Reference(args.data[0]);
		return;
	}
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LeastGreatestCollationState>();
	const auto count = args.size();
	lstate.collation_keys.Reset();
	lstate.value_keys.Reset();

	auto result_type = VectorType::CONSTANT_VECTOR;
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		auto &input = args.data[col_idx];
		if (input.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result_type = VectorType::FLAT_VECTOR;
		}
		lstate.collation_input.Reset();
		lstate.collation_input.data[0].Reference(input);
		lstate.collation_input.CheckCardinality(count);
		lstate.collated.Reset();
		auto &collated = lstate.collated.data[0];
		lstate.executor.ExecuteExpression(lstate.collation_input, collated);
		CreateSortKeyHelpers::CreateSortKey(collated, lstate.modifiers, lstate.collation_keys.data[col_idx]);
		CreateSortKeyHelpers::CreateSortKey(input, lstate.modifiers, lstate.value_keys.data[col_idx]);
	}

	// NULL arguments are skipped, and on equal keys the first argument wins
	auto &winners = lstate.winners;
	auto winner_keys = FlatVector::ScatterWriter<string_t>(lstate.winner_keys);
	{
		auto validity = args.data[0].Validity();
		auto keys = lstate.collation_keys.data[0].Values<string_t>();
		for (idx_t i = 0; i < count; i++) {
			if (!validity.IsValid(i)) {
				winners[i] = DConstants::INVALID_INDEX;
				continue;
			}
			winners[i] = 0;
			winner_keys[i] = keys[i].GetValue();
		}
	}
	for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
		auto validity = args.data[col_idx].Validity();
		auto keys = lstate.collation_keys.data[col_idx].Values<string_t>();
		for (idx_t i = 0; i < count; i++) {
			if (!validity.IsValid(i)) {
				continue;
			}
			auto key = keys[i].GetValue();
			if (winners[i] == DConstants::INVALID_INDEX || OP::template Operation<string_t>(key, winner_keys[i])) {
				winners[i] = col_idx;
				winner_keys[i] = key;
			}
		}
	}

	auto &result_mask = FlatVector::ValidityMutable(result);
	for (idx_t i = 0; i < count; i++) {
		if (winners[i] == DConstants::INVALID_INDEX) {
			result_mask.SetInvalid(i);
		}
	}
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		auto values = lstate.value_keys.data[col_idx].Values<string_t>();
		for (idx_t i = 0; i < count; i++) {
			if (winners[i] == col_idx) {
				CreateSortKeyHelpers::DecodeSortKey(values[i].GetValue(), result, i, lstate.modifiers);
			}
		}
	}
	result.SetVectorType(result_type);
}

template <class LEAST_GREATER_OP>
unique_ptr<BaseStatistics> PropagateLeastGreatestStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto &return_type = expr.GetReturnType();
	// with a collation (bind data), the child min/max do not follow the order the result is chosen by
	if (input.bind_data || BaseStatistics::GetStatsType(return_type) != StatisticsType::NUMERIC_STATS ||
	    return_type.InternalType() == PhysicalType::BOOL) {
		return nullptr;
	}
	constexpr bool IS_LEAST = std::is_same_v<LEAST_GREATER_OP, LeastOp>;

	auto merge = [](Value &bound, const Value &candidate, bool keep_smaller) {
		if (bound.IsNull() || (keep_smaller ? candidate < bound : candidate > bound)) {
			bound = candidate;
		}
	};

	// least/greatest always returns one of its inputs, so the result range is built from the
	// inputs' min/max values. A NULL input is skipped at runtime, so only inputs that can never
	// be NULL may tighten the "anchored" side of the range:
	//
	//             | loose side (over all inputs) | anchored side (over non-null inputs)
	//   least     | min of the mins  -> min      | min of the maxes -> max
	//   greatest  | max of the maxes -> max      | max of the mins  -> min
	//
	// If every input can be NULL, a single input may end up deciding the result, so the anchored
	// side falls back to the opposite aggregation over all inputs
	Value loose;
	Value anchored;          // over non-null inputs only
	Value anchored_fallback; // over all inputs; used only when every input is nullable
	bool has_nonnull_input = false;
	for (auto &cs : child_stats) {
		if (cs.GetStatsType() != StatisticsType::NUMERIC_STATS || !NumericStats::HasMinMax(cs)) {
			return nullptr;
		}
		const Value cmin = NumericStats::Min(cs);
		const Value cmax = NumericStats::Max(cs);
		const Value &loose_val = IS_LEAST ? cmin : cmax;
		const Value &anchored_val = IS_LEAST ? cmax : cmin;

		merge(loose, loose_val, /*keep_smaller=*/IS_LEAST);
		merge(anchored_fallback, anchored_val, /*keep_smaller=*/!IS_LEAST);
		if (!cs.CanHaveNull()) {
			has_nonnull_input = true;
			merge(anchored, anchored_val, /*keep_smaller=*/IS_LEAST);
		}
	}
	if (!has_nonnull_input) {
		anchored = std::move(anchored_fallback);
	}

	auto result = NumericStats::CreateEmpty(return_type);
	if constexpr (IS_LEAST) {
		NumericStats::SetMin(result, loose);
		NumericStats::SetMax(result, anchored);
	} else {
		NumericStats::SetMin(result, anchored);
		NumericStats::SetMax(result, loose);
	}
	result.Set(StatsInfo::CAN_HAVE_VALID_VALUES);
	if (!has_nonnull_input) {
		// the result is NULL only if all inputs are NULL
		result.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	}
	return result.ToUnique();
}

template <class LEAST_GREATER_OP>
void LeastGreatestResolveTypes(ResolveScalarFunctionTypesInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	LogicalType child_type = ExpressionBinder::GetExpressionReturnType(input.GetArgument(0));
	for (idx_t i = 1; i < input.GetArgumentCount(); i++) {
		auto arg_type = ExpressionBinder::GetExpressionReturnType(input.GetArgument(i));
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, arg_type, child_type,
		                                       input.GetDecimalWidthLimit())) {
			throw BinderException(input.GetArgument(i).GetQueryLocation(),
			                      "Cannot combine types of %s and %s - an explicit cast is required",
			                      child_type.ToString(), arg_type.ToString());
		}
	}
	switch (child_type.id()) {
	case LogicalTypeId::UNKNOWN:
		throw ParameterNotResolvedException();
	case LogicalTypeId::INTEGER_LITERAL:
		child_type = IntegerLiteral::GetType(child_type);
		break;
	case LogicalTypeId::STRING_LITERAL:
		child_type = LogicalType::VARCHAR;
		break;
	default:
		break;
	}
	using OP = typename LEAST_GREATER_OP::OP;
	switch (child_type.InternalType()) {
#if !DUCKDB_SMALLER_BINARY(least_greatest_types)
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int8_t, OP>);
		break;
	case PhysicalType::INT16:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int16_t, OP>);
		break;
	case PhysicalType::INT32:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int32_t, OP>);
		break;
	case PhysicalType::INT64:
		bound_function.SetFunctionCallback(LeastGreatestFunction<int64_t, OP>);
		break;
	case PhysicalType::INT128:
		bound_function.SetFunctionCallback(LeastGreatestFunction<hugeint_t, OP>);
		break;
	case PhysicalType::DOUBLE:
		bound_function.SetFunctionCallback(LeastGreatestFunction<double, OP>);
		break;
	case PhysicalType::VARCHAR:
		bound_function.SetFunctionCallback(LeastGreatestFunction<string_t, OP, StandardLeastGreatest<true>>);
		break;
#endif
	default:
		// fallback with sort keys
		bound_function.SetFunctionCallback(LeastGreatestFunction<string_t, OP, SortKeyLeastGreatest>);
		bound_function.SetInitStateCallback(LeastGreatestSortKeyInit<LEAST_GREATER_OP>);
		break;
	}
	for (auto &arg : bound_function.GetArguments()) {
		arg = child_type;
	}
	bound_function.SetReturnType(child_type);
}

//! Types whose order differs from their physical representation (e.g. TIMETZ, INTERVAL, collated VARCHAR) are
//! compared through their collation, like comparisons and ORDER BY
template <class LEAST_GREATER_OP>
unique_ptr<FunctionData> LeastGreatestBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto child_type = bound_function.GetReturnType();
	unique_ptr<Expression> collation = make_uniq<BoundReferenceExpression>(child_type, 0ULL);
	if (!ExpressionBinder::PushCollation(input.GetClientContext(), collation, child_type)) {
		return nullptr;
	}
	bound_function.SetFunctionCallback(LeastGreatestCollationFunction<typename LEAST_GREATER_OP::OP>);
	bound_function.SetInitStateCallback(LeastGreatestCollationInit);
	return make_uniq<LeastGreatestCollationData>(std::move(collation));
}

template <class OP>
ScalarFunction GetLeastGreatestFunction() {
	ScalarFunction fun({{"arg1", LogicalType::ANY}}, LogicalType::ANY, nullptr, nullptr,
	                   PropagateLeastGreatestStats<OP>, nullptr, LogicalType::ANY, FunctionStability::CONSISTENT,
	                   FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetResolveTypesCallback(LeastGreatestResolveTypes<OP>);
	fun.SetBindCallback(LeastGreatestBind<OP>);
	return fun;
}

template <class OP>
ScalarFunctionSet GetLeastGreatestFunctions() {
	ScalarFunctionSet fun_set;
	fun_set.AddFunction(GetLeastGreatestFunction<OP>());
	return fun_set;
}

} // namespace

ScalarFunctionSet LeastFun::GetFunctions() {
	return GetLeastGreatestFunctions<LeastOp>();
}

ScalarFunctionSet GreatestFun::GetFunctions() {
	return GetLeastGreatestFunctions<GreaterOp>();
}

} // namespace duckdb
