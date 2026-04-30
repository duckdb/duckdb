#include "duckdb/common/clustered_aggr.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

namespace {

template <class T>
struct FirstState {
	using VALUE_TYPE = T;
	T value;
	bool is_set;
	bool is_null;
};

struct FirstFunctionBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.is_set = false;
		state.is_null = false;
	}

	static bool IgnoreNull() {
		return false;
	}
};

template <bool LAST, class FUNC>
static inline void ScanClusterRange(idx_t pos, idx_t end, FUNC &&func) {
	if constexpr (LAST) {
		for (idx_t k = end; k > pos; k--) {
			if (func(k - 1)) {
				return;
			}
		}
	} else {
		for (idx_t k = pos; k < end; k++) {
			if (func(k)) {
				return;
			}
		}
	}
}

template <bool LAST, bool SKIP_NULLS>
struct FirstFunction : public FirstFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			if (!unary_input.RowIsValid()) {
				if (!SKIP_NULLS) {
					state.is_set = true;
				}
				state.is_null = true;
			} else {
				state.is_set = true;
				state.is_null = false;
				state.value = input;
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	// Clustered early-exit: first/any_value scan forward and stop at the first valid value;
	// last scans backward and stops at the first valid value from the end.
	template <class INPUT_TYPE, class STATE_TYPE, class OP, bool ALL_VALID>
	static void ClusteredOperationInternal(STATE_TYPE &state, const INPUT_TYPE *vals, const sel_t *sel,
	                                       const SelectionVector &isel, const ValidityMask &validity, idx_t pos,
	                                       idx_t end) {
		if (!LAST && state.is_set) {
			return;
		}
		ScanClusterRange<LAST>(pos, end, [&](idx_t k) {
			auto idx = isel.get_index(sel[k]);
			if (ALL_VALID || validity.RowIsValidUnsafe(idx)) {
				state.is_set = true;
				state.is_null = false;
				state.value = vals[idx];
				return true;
			}
			if (!SKIP_NULLS) {
				state.is_set = true;
				state.is_null = true;
				return true;
			}
			return false;
		});
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void ClusteredOperation(STATE_TYPE &state, const INPUT_TYPE *vals, AggregateUnaryInput &unary_input,
	                               const sel_t *sel, const SelectionVector &isel, const ValidityMask &validity,
	                               idx_t pos,
	                               idx_t end) {
		if (validity.CanHaveNull()) {
			ClusteredOperationInternal<INPUT_TYPE, STATE_TYPE, OP, false>(state, vals, sel, isel, validity, pos, end);
		} else {
			ClusteredOperationInternal<INPUT_TYPE, STATE_TYPE, OP, true>(state, vals, sel, isel, validity, pos, end);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!target.is_set) {
			target = source;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionStringBase : public FirstFunctionBase {
	template <class STATE, bool COMBINE = false>
	static void SetValue(STATE &state, AggregateInputData &input_data, string_t value, bool is_null) {
		if (LAST && state.is_set) {
			Destroy(state, input_data);
		}
		if (is_null) {
			if (!SKIP_NULLS) {
				state.is_set = true;
				state.is_null = true;
			}
		} else {
			state.is_set = true;
			state.is_null = false;
			if ((COMBINE && !LAST) || value.IsInlined()) {
				// We use the aggregate allocator for 'first', so the allocation is already done when combining
				// Of course, if the value is inlined, we also don't need to allocate
				state.value = value;
			} else {
				// non-inlined string, need to allocate space for it
				auto len = value.GetSize();
				auto ptr = LAST ? new char[len] : char_ptr_cast(input_data.allocator.Allocate(len));
				memcpy(ptr, value.GetData(), len);

				state.value = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (source.is_set && (LAST || !target.is_set)) {
			SetValue<STATE, true>(target, input_data, source.value, source.is_null);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.is_set && !state.is_null && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionString : FirstFunctionStringBase<LAST, SKIP_NULLS> {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE>(state, unary_input.input, input,
			                                                                    !unary_input.RowIsValid());
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP, bool ALL_VALID>
	static void ClusteredOperationInternal(STATE_TYPE &state, const INPUT_TYPE *vals, AggregateUnaryInput &unary_input,
	                                       const sel_t *sel, const SelectionVector &isel, const ValidityMask &validity,
	                                       idx_t pos, idx_t end) {
		if (!LAST && state.is_set) {
			return;
		}
		ScanClusterRange<LAST>(pos, end, [&](idx_t k) {
			auto idx = isel.get_index(sel[k]);
			bool is_null = ALL_VALID ? false : !validity.RowIsValidUnsafe(idx);
			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE_TYPE>(state, unary_input.input, vals[idx],
			                                                                         is_null);
			return state.is_set;
		});
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void ClusteredOperation(STATE_TYPE &state, const INPUT_TYPE *vals, AggregateUnaryInput &unary_input,
	                               const sel_t *sel, const SelectionVector &isel, const ValidityMask &validity,
	                               idx_t pos,
	                               idx_t end) {
		if (validity.CanHaveNull()) {
			ClusteredOperationInternal<INPUT_TYPE, STATE_TYPE, OP, false>(state, vals, unary_input, sel, isel, validity,
			                                                              pos, end);
		} else {
			ClusteredOperationInternal<INPUT_TYPE, STATE_TYPE, OP, true>(state, vals, unary_input, sel, isel, validity,
			                                                             pos, end);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstVectorFunction : FirstFunctionStringBase<LAST, SKIP_NULLS> {
	using STATE = FirstState<string_t>;

	static void Update(Vector inputs[], AggregateInputData &input_data, idx_t, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		sel_t assign_sel[STANDARD_VECTOR_SIZE];
		idx_t assign_count = 0;

		auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			const auto idx = idata.sel->get_index(i);
			bool is_null = !idata.validity.RowIsValid(idx);
			if (SKIP_NULLS && is_null) {
				continue;
			}
			auto &state = *states[sdata.sel->get_index(i)];
			if (!LAST && state.is_set) {
				continue;
			}
			assign_sel[assign_count++] = NumericCast<sel_t>(i);
		}
		if (assign_count == 0) {
			// fast path - nothing to set
			return;
		}

		Vector sort_key(LogicalType::BLOB);
		OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
		// slice with a selection vector and generate sort keys
		if (assign_count == count) {
			CreateSortKeyHelpers::CreateSortKey(input, count, modifiers, sort_key);
		} else {
			SelectionVector sel(assign_sel, STANDARD_VECTOR_SIZE);
			Vector sliced_input(input, sel, assign_count);
			CreateSortKeyHelpers::CreateSortKey(sliced_input, assign_count, modifiers, sort_key);
		}
		auto sort_key_data = FlatVector::GetData<string_t>(sort_key);

		// now assign sort keys
		for (idx_t i = 0; i < assign_count; i++) {
			const auto state_idx = sdata.sel->get_index(assign_sel[i]);
			auto &state = *states[state_idx];
			if (!LAST && state.is_set) {
				continue;
			}

			const auto idx = idata.sel->get_index(assign_sel[i]);
			bool is_null = !idata.validity.RowIsValid(idx);
			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE>(state, input_data, sort_key_data[i],
			                                                                    is_null);
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			CreateSortKeyHelpers::DecodeSortKey(state.value, finalize_data.result, finalize_data.result_idx,
			                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
		}
	}

	static unique_ptr<FunctionData> Bind(BindAggregateFunctionInput &input) {
		auto &function = input.GetBoundFunction();
		auto &arguments = input.GetArguments();

		function.GetArguments()[0] = arguments[0]->return_type;
		function.SetReturnType(arguments[0]->return_type);
		return nullptr;
	}
};

LogicalType GetFirstStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> child_types;
	LogicalType value_type = function.GetArguments()[0];
	child_types.emplace_back("value", value_type);
	child_types.emplace_back("is_set", LogicalType::BOOLEAN);
	child_types.emplace_back("is_null", LogicalType::BOOLEAN);
	return LogicalType::STRUCT(std::move(child_types));
}

template <class T, bool LAST, bool SKIP_NULLS>
void FirstFunctionClusterUpdate(Vector inputs[], AggregateInputData &aggregate_input_data, idx_t input_count,
                                const ClusteredAggr &clustered, idx_t count) {
	D_ASSERT(input_count == 1);
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(count, idata);
	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	AggregateUnaryInput unary_input(aggregate_input_data, idata.validity);
	for (idx_t r = 0; r < clustered.n_group_runs; r++) {
		auto &state = *reinterpret_cast<FirstState<T> *>(clustered.group_runs[r].state);
		const auto *run_sel = clustered.group_runs[r].sel;
		const auto run_count = clustered.group_runs[r].count;
		FirstFunction<LAST, SKIP_NULLS>::template ClusteredOperation<T, FirstState<T>, FirstFunction<LAST, SKIP_NULLS>>(
		    state, input_data, unary_input, run_sel, *idata.sel, idata.validity, 0, run_count);
	}
}

template <class T, bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstAggregateTemplated(const LogicalType &type) {
	auto result = AggregateFunction({type}, type, AggregateFunction::StateSize<FirstState<T>>,
	                                AggregateFunction::StateInitialize<FirstState<T>, FirstFunction<LAST, SKIP_NULLS>>,
	                                AggregateFunction::UnaryScatterUpdate<FirstState<T>, T,
	                                                                     FirstFunction<LAST, SKIP_NULLS>>,
	                                AggregateFunction::StateCombine<FirstState<T>, FirstFunction<LAST, SKIP_NULLS>>,
	                                AggregateFunction::StateFinalize<FirstState<T>, T,
	                                                                 FirstFunction<LAST, SKIP_NULLS>>,
	                                FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                                FirstFunctionClusterUpdate<T, LAST, SKIP_NULLS>);
	result.SetStructStateExport(GetFirstStateType);
	return result;
}

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstFunction(const LogicalType &type);

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetDecimalFirstFunction(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::SMALLINT);
	case PhysicalType::INT32:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::INTEGER);
	case PhysicalType::INT64:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::BIGINT);
	default:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::HUGEINT);
	}
}
template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstFunction(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		type.Verify();
		AggregateFunction function = GetDecimalFirstFunction<LAST, SKIP_NULLS>(type);
		function.GetArguments()[0] = type;
		function.SetReturnType(type);
		return function;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetFirstAggregateTemplated<int8_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT16:
		return GetFirstAggregateTemplated<int16_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT32:
		return GetFirstAggregateTemplated<int32_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT64:
		return GetFirstAggregateTemplated<int64_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT8:
		return GetFirstAggregateTemplated<uint8_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT16:
		return GetFirstAggregateTemplated<uint16_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT32:
		return GetFirstAggregateTemplated<uint32_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT64:
		return GetFirstAggregateTemplated<uint64_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::INT128:
		return GetFirstAggregateTemplated<hugeint_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::UINT128:
		return GetFirstAggregateTemplated<uhugeint_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::FLOAT:
		return GetFirstAggregateTemplated<float, LAST, SKIP_NULLS>(type);
	case PhysicalType::DOUBLE:
		return GetFirstAggregateTemplated<double, LAST, SKIP_NULLS>(type);
	case PhysicalType::INTERVAL:
		return GetFirstAggregateTemplated<interval_t, LAST, SKIP_NULLS>(type);
	case PhysicalType::VARCHAR:
		if (LAST) {
			auto fun = AggregateFunction::UnaryAggregateDestructor<FirstState<string_t>, string_t, string_t,
			                                                       FirstFunctionString<LAST, SKIP_NULLS>>(type, type);
			fun.SetStructStateExport(GetFirstStateType);
			return fun;
		} else {
			auto fun = AggregateFunction::UnaryAggregate<FirstState<string_t>, string_t, string_t,
			                                             FirstFunctionString<LAST, SKIP_NULLS>>(type, type);
			fun.SetStructStateExport(GetFirstStateType);
			return fun;
		}
	default: {
		using OP = FirstVectorFunction<LAST, SKIP_NULLS>;
		using STATE = FirstState<string_t>;
		auto fun = AggregateFunction(
		    {type}, type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
		    OP::Update, AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateVoidFinalize<STATE, OP>,
		    FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, OP::Bind,
		    LAST ? AggregateFunction::StateDestroy<STATE, OP> : nullptr, nullptr, nullptr);
		fun.SetStructStateExport(GetFirstStateType);
		return fun;
	}
	}
}

template <bool LAST, bool SKIP_NULLS>
unique_ptr<FunctionData> BindDecimalFirst(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto decimal_type = arguments[0]->return_type;
	auto name = std::move(function.name);
	function = GetFirstFunction<LAST, SKIP_NULLS>(decimal_type);
	function.name = std::move(name);
	function.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	function.SetReturnType(decimal_type);
	return nullptr;
}

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetFirstOperator(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		throw InternalException("FIXME: this shouldn't happen...");
	}
	return GetFirstFunction<LAST, SKIP_NULLS>(type);
}

template <bool LAST, bool SKIP_NULLS>
unique_ptr<FunctionData> BindFirst(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto input_type = arguments[0]->return_type;
	auto name = std::move(function.name);
	function = GetFirstOperator<LAST, SKIP_NULLS>(input_type);
	function.name = std::move(name);
	function.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	if (function.HasBindCallback()) {
		return function.Bind(input.GetClientContext(), arguments);
		;
	} else {
		return nullptr;
	}
}

template <bool LAST, bool SKIP_NULLS>
void AddFirstOperator(AggregateFunctionSet &set) {
	set.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                  BindDecimalFirst<LAST, SKIP_NULLS>));
	set.AddFunction(AggregateFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                  BindFirst<LAST, SKIP_NULLS>));
}

} // namespace

AggregateFunction FirstFunctionGetter::GetFunction(const LogicalType &type) {
	auto fun = GetFirstFunction<false, false>(type);
	fun.name = "first";
	return fun;
}

AggregateFunction LastFunctionGetter::GetFunction(const LogicalType &type) {
	auto fun = GetFirstFunction<true, false>(type);
	fun.name = "last";
	return fun;
}

AggregateFunctionSet FirstFun::GetFunctions() {
	AggregateFunctionSet first("first");
	AddFirstOperator<false, false>(first);
	return first;
}

AggregateFunctionSet LastFun::GetFunctions() {
	AggregateFunctionSet last("last");
	AddFirstOperator<true, false>(last);
	return last;
}

AggregateFunctionSet AnyValueFun::GetFunctions() {
	AggregateFunctionSet any_value("any_value");
	AddFirstOperator<false, true>(any_value);
	return any_value;
}

} // namespace duckdb
