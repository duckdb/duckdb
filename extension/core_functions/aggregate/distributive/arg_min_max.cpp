#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/function/aggregate/minmax_n_helpers.hpp"

namespace duckdb {

namespace {

struct ArgMinMaxStateBase {
	ArgMinMaxStateBase() : is_initialized(false), arg_null(false), val_null(false) {
	}

	template <class T>
	static inline void CreateValue(T &value) {
	}

	template <class T>
	static inline void AssignValue(T &target, T new_value, AggregateInputData &aggregate_input_data) {
		target = new_value;
	}

	template <typename T>
	static inline void ReadValue(Vector &result, T &arg, T &target) {
		target = arg;
	}

	bool is_initialized;
	bool arg_null;
	bool val_null;
};

// Out-of-line specialisations
template <>
void ArgMinMaxStateBase::CreateValue(string_t &value) {
	value = string_t(uint32_t(0));
}

template <>
void ArgMinMaxStateBase::AssignValue(string_t &target, string_t new_value, AggregateInputData &aggregate_input_data) {
	if (new_value.IsInlined()) {
		target = new_value;
	} else {
		// non-inlined string, need to allocate space for it
		auto len = new_value.GetSize();
		char *ptr;
		if (!target.IsInlined() && target.GetSize() >= len) {
			// Target has enough space, reuse ptr
			ptr = target.GetPointer();
		} else {
			// Target might be too small, allocate
			ptr = reinterpret_cast<char *>(aggregate_input_data.allocator.Allocate(len));
		}
		memcpy(ptr, new_value.GetData(), len);
		target = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
	}
}

template <>
void ArgMinMaxStateBase::ReadValue(Vector &result, string_t &arg, string_t &target) {
	target = StringVector::AddStringOrBlob(result, arg);
}

template <class A, class B>
struct ArgMinMaxState : public ArgMinMaxStateBase {
	using ARG_TYPE = A;
	using BY_TYPE = B;

	ARG_TYPE arg;
	BY_TYPE value;

	ArgMinMaxState() {
		CreateValue(arg);
		CreateValue(value);
	}
};

template <class COMPARATOR>
struct ArgMinMaxBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Assign(STATE &state, const A_TYPE &x, const B_TYPE &y, const bool x_null, const bool y_null,
	                   AggregateInputData &aggregate_input_data) {
		D_ASSERT(aggregate_input_data.bind_data);
		const auto &bind_data = aggregate_input_data.bind_data->Cast<ArgMinMaxFunctionData>();

		if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL) {
			STATE::template AssignValue<A_TYPE>(state.arg, x, aggregate_input_data);
			STATE::template AssignValue<B_TYPE>(state.value, y, aggregate_input_data);
		} else {
			state.arg_null = x_null;
			state.val_null = y_null;
			if (!state.arg_null) {
				STATE::template AssignValue<A_TYPE>(state.arg, x, aggregate_input_data);
			}
			if (!state.val_null) {
				STATE::template AssignValue<B_TYPE>(state.value, y, aggregate_input_data);
			}
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &x, const B_TYPE &y, AggregateBinaryInput &binary) {
		D_ASSERT(binary.input.bind_data);
		const auto &bind_data = binary.input.bind_data->Cast<ArgMinMaxFunctionData>();
		if (!state.is_initialized) {
			if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL &&
			    binary.left_mask.RowIsValid(binary.lidx) && binary.right_mask.RowIsValid(binary.ridx)) {
				Assign(state, x, y, !binary.left_mask.RowIsValid(binary.lidx),
				       !binary.right_mask.RowIsValid(binary.ridx), binary.input);
				state.is_initialized = true;
				return;
			}
			if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ARG_NULL &&
			    binary.right_mask.RowIsValid(binary.ridx)) {
				Assign(state, x, y, !binary.left_mask.RowIsValid(binary.lidx),
				       !binary.right_mask.RowIsValid(binary.ridx), binary.input);
				state.is_initialized = true;
				return;
			}
			if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ANY_NULL) {
				Assign(state, x, y, !binary.left_mask.RowIsValid(binary.lidx),
				       !binary.right_mask.RowIsValid(binary.ridx), binary.input);
				state.is_initialized = true;
			}
		} else {
			OP::template Execute<A_TYPE, B_TYPE, STATE>(state, x, y, binary);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Execute(STATE &state, A_TYPE x_data, B_TYPE y_data, AggregateBinaryInput &binary) {
		D_ASSERT(binary.input.bind_data);
		const auto &bind_data = binary.input.bind_data->Cast<ArgMinMaxFunctionData>();

		if (binary.right_mask.RowIsValid(binary.ridx) &&
		    (state.val_null || COMPARATOR::Operation(y_data, state.value))) {
			if (bind_data.null_handling != ArgMinMaxNullHandling::IGNORE_ANY_NULL ||
			    binary.left_mask.RowIsValid(binary.lidx)) {
				Assign(state, x_data, y_data, !binary.left_mask.RowIsValid(binary.lidx), false, binary.input);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggregate_input_data) {
		if (!source.is_initialized) {
			return;
		}

		if (!target.is_initialized || target.val_null ||
		    (!source.val_null && COMPARATOR::Operation(source.value, target.value))) {
			Assign(target, source.arg, source.value, source.arg_null, false, aggregate_input_data);
			target.is_initialized = true;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized || state.arg_null) {
			finalize_data.ReturnNull();
		} else {
			STATE::template ReadValue<T>(finalize_data.result, state.arg, target);
		}
	}

	static bool IgnoreNull() {
		return false;
	}

	template <ArgMinMaxNullHandling NULL_HANDLING>
	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		if (arguments[1]->return_type.InternalType() == PhysicalType::VARCHAR) {
			ExpressionBinder::PushCollation(context, arguments[1], arguments[1]->return_type);
		}
		function.arguments[0] = arguments[0]->return_type;
		function.SetReturnType(arguments[0]->return_type);

		auto function_data = make_uniq<ArgMinMaxFunctionData>(NULL_HANDLING);
		return unique_ptr<FunctionData>(std::move(function_data));
	}
};

struct SpecializedGenericArgMinMaxState {
	static bool CreateExtraState(idx_t count) {
		// nop extra state
		return false;
	}

	static void PrepareData(Vector &by, idx_t count, bool &, UnifiedVectorFormat &result) {
		by.ToUnifiedFormat(count, result);
	}
};

template <OrderType ORDER_TYPE>
struct GenericArgMinMaxState {
	static Vector CreateExtraState(idx_t count) {
		return Vector(LogicalType::BLOB, count);
	}

	static void PrepareData(Vector &by, idx_t count, Vector &extra_state, UnifiedVectorFormat &result) {
		OrderModifiers modifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST);
		CreateSortKeyHelpers::CreateSortKeyWithValidity(by, extra_state, modifiers, count);
		extra_state.ToUnifiedFormat(count, result);
	}
};

template <typename COMPARATOR, OrderType ORDER_TYPE, class UPDATE_TYPE = SpecializedGenericArgMinMaxState>
struct VectorArgMinMaxBase : ArgMinMaxBase<COMPARATOR> {
	template <class STATE>
	static void Update(Vector inputs[], AggregateInputData &aggregate_input_data, idx_t input_count,
	                   Vector &state_vector, idx_t count) {
		D_ASSERT(aggregate_input_data.bind_data);
		const auto &bind_data = aggregate_input_data.bind_data->Cast<ArgMinMaxFunctionData>();

		auto &arg = inputs[0];
		UnifiedVectorFormat adata;
		arg.ToUnifiedFormat(count, adata);

		using ARG_TYPE = typename STATE::ARG_TYPE;
		using BY_TYPE = typename STATE::BY_TYPE;
		auto &by = inputs[1];
		UnifiedVectorFormat bdata;
		auto extra_state = UPDATE_TYPE::CreateExtraState(count);
		UPDATE_TYPE::PrepareData(by, count, extra_state, bdata);
		const auto bys = UnifiedVectorFormat::GetData<BY_TYPE>(bdata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		STATE *last_state = nullptr;
		sel_t assign_sel[STANDARD_VECTOR_SIZE];
		idx_t assign_count = 0;

		auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			const auto sidx = sdata.sel->get_index(i);
			auto &state = *states[sidx];

			const auto aidx = adata.sel->get_index(i);
			const auto arg_null = !adata.validity.RowIsValid(aidx);

			if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL && arg_null) {
				continue;
			}

			const auto bidx = bdata.sel->get_index(i);

			if (!bdata.validity.RowIsValid(bidx)) {
				if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ANY_NULL && !state.is_initialized) {
					state.val_null = true;
					if (!arg_null) {
						state.is_initialized = true;
						if (&state == last_state) {
							assign_count--;
						}
						assign_sel[assign_count++] = UnsafeNumericCast<sel_t>(i);
						last_state = &state;
					}
				}
				continue;
			}

			const auto bval = bys[bidx];

			if (!state.is_initialized || state.val_null || COMPARATOR::template Operation<BY_TYPE>(bval, state.value)) {
				STATE::template AssignValue<BY_TYPE>(state.value, bval, aggregate_input_data);
				state.arg_null = arg_null;
				// micro-adaptivity: it is common we overwrite the same state repeatedly
				// e.g. when running arg_max(val, ts) and ts is sorted in ascending order
				// this check essentially says:
				// "if we are overriding the same state as the last row, the last write was pointless"
				// hence we skip the last write altogether
				if (!arg_null) {
					if (&state == last_state) {
						assign_count--;
					}
					assign_sel[assign_count++] = UnsafeNumericCast<sel_t>(i);
					last_state = &state;
				}
				state.is_initialized = true;
			}
		}
		if (assign_count == 0) {
			// no need to assign anything: nothing left to do
			return;
		}
		Vector sort_key(LogicalType::BLOB);
		auto modifiers = OrderModifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST);
		// slice with a selection vector and generate sort keys
		SelectionVector sel(assign_sel);
		Vector sliced_input(arg, sel, assign_count);
		CreateSortKeyHelpers::CreateSortKey(sliced_input, assign_count, modifiers, sort_key);
		auto sort_key_data = FlatVector::GetData<string_t>(sort_key);

		// now assign sort keys
		for (idx_t i = 0; i < assign_count; i++) {
			const auto sidx = sdata.sel->get_index(sel.get_index(i));
			auto &state = *states[sidx];
			STATE::template AssignValue<ARG_TYPE>(state.arg, sort_key_data[i], aggregate_input_data);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggregate_input_data) {
		if (!source.is_initialized) {
			return;
		}
		if (!target.is_initialized || target.val_null ||
		    (!source.val_null && COMPARATOR::Operation(source.value, target.value))) {
			target.val_null = source.val_null;
			if (!target.val_null) {
				STATE::template AssignValue<typename STATE::BY_TYPE>(target.value, source.value, aggregate_input_data);
			}
			target.arg_null = source.arg_null;
			if (!target.arg_null) {
				STATE::template AssignValue<typename STATE::ARG_TYPE>(target.arg, source.arg, aggregate_input_data);
			}
			target.is_initialized = true;
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized || state.arg_null) {
			finalize_data.ReturnNull();
		} else {
			CreateSortKeyHelpers::DecodeSortKey(state.arg, finalize_data.result, finalize_data.result_idx,
			                                    OrderModifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST));
		}
	}

	template <ArgMinMaxNullHandling NULL_HANDLING>
	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		if (arguments[1]->return_type.InternalType() == PhysicalType::VARCHAR) {
			ExpressionBinder::PushCollation(context, arguments[1], arguments[1]->return_type);
		}
		function.arguments[0] = arguments[0]->return_type;
		function.SetReturnType(arguments[0]->return_type);

		auto function_data = make_uniq<ArgMinMaxFunctionData>(NULL_HANDLING);
		return unique_ptr<FunctionData>(std::move(function_data));
	}
};

template <class OP>
bind_aggregate_function_t GetBindFunction(const ArgMinMaxNullHandling null_handling) {
	switch (null_handling) {
	case ArgMinMaxNullHandling::HANDLE_ARG_NULL:
		return OP::template Bind<ArgMinMaxNullHandling::HANDLE_ARG_NULL>;
	case ArgMinMaxNullHandling::HANDLE_ANY_NULL:
		return OP::template Bind<ArgMinMaxNullHandling::HANDLE_ANY_NULL>;
	default:
		return OP::template Bind<ArgMinMaxNullHandling::IGNORE_ANY_NULL>;
	}
}

template <class OP>
AggregateFunction GetGenericArgMinMaxFunction(const ArgMinMaxNullHandling null_handling) {
	using STATE = ArgMinMaxState<string_t, string_t>;
	auto bind = GetBindFunction<OP>(null_handling);
	return AggregateFunction(
	    {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, AggregateFunction::StateSize<STATE>,
	    AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>, OP::template Update<STATE>,
	    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, bind,
	    AggregateFunction::StateDestroy<STATE, OP>);
}

template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunction GetVectorArgMinMaxFunctionInternal(const LogicalType &by_type, const LogicalType &type,
                                                     const ArgMinMaxNullHandling null_handling) {
#ifndef DUCKDB_SMALLER_BINARY
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	auto bind = GetBindFunction<OP>(null_handling);
	return AggregateFunction({type, by_type}, type, AggregateFunction::StateSize<STATE>,
	                         AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>,
	                         OP::template Update<STATE>, AggregateFunction::StateCombine<STATE, OP>,
	                         AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, bind,
	                         AggregateFunction::StateDestroy<STATE, OP>);
#else
	auto function = GetGenericArgMinMaxFunction<OP>(null_handling);
	function.arguments = {type, by_type};
	function.return_type = type;
	return function;
#endif
}

#ifndef DUCKDB_SMALLER_BINARY
template <class OP, class ARG_TYPE>
AggregateFunction GetVectorArgMinMaxFunctionBy(const LogicalType &by_type, const LogicalType &type,
                                               const ArgMinMaxNullHandling null_handling) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, int32_t>(by_type, type, null_handling);
	case PhysicalType::INT64:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, int64_t>(by_type, type, null_handling);
	case PhysicalType::INT128:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, hugeint_t>(by_type, type, null_handling);
	case PhysicalType::DOUBLE:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, double>(by_type, type, null_handling);
	case PhysicalType::VARCHAR:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, string_t>(by_type, type, null_handling);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}
#endif

const vector<LogicalType> ArgMaxByTypes() {
	vector<LogicalType> types = {LogicalType::INTEGER,   LogicalType::BIGINT,       LogicalType::HUGEINT,
	                             LogicalType::DOUBLE,    LogicalType::VARCHAR,      LogicalType::DATE,
	                             LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ, LogicalType::BLOB};
	return types;
}

template <class OP, class ARG_TYPE>
void AddVectorArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type,
                                  const ArgMinMaxNullHandling null_handling) {
	auto by_types = ArgMaxByTypes();
	for (const auto &by_type : by_types) {
#ifndef DUCKDB_SMALLER_BINARY
		fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(by_type, type, null_handling));
#else
		fun.AddFunction(GetVectorArgMinMaxFunctionInternal<OP, string_t, string_t>(by_type, type, null_handling));
#endif
	}
}

template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunction GetArgMinMaxFunctionInternal(const LogicalType &by_type, const LogicalType &type,
                                               const ArgMinMaxNullHandling null_handling) {
#ifndef DUCKDB_SMALLER_BINARY
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	auto function =
	    AggregateFunction::BinaryAggregate<STATE, ARG_TYPE, BY_TYPE, ARG_TYPE, OP, AggregateDestructorType::LEGACY>(
	        type, by_type, type);
	if (type.InternalType() == PhysicalType::VARCHAR || by_type.InternalType() == PhysicalType::VARCHAR) {
		function.destructor = AggregateFunction::StateDestroy<STATE, OP>;
	}
	function.bind = GetBindFunction<OP>(null_handling);
#else
	auto function = GetGenericArgMinMaxFunction<OP>(null_handling);
	function.arguments = {type, by_type};
	function.return_type = type;
#endif
	return function;
}

#ifndef DUCKDB_SMALLER_BINARY
template <class OP, class ARG_TYPE>
AggregateFunction GetArgMinMaxFunctionBy(const LogicalType &by_type, const LogicalType &type,
                                         const ArgMinMaxNullHandling null_handling) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int32_t>(by_type, type, null_handling);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int64_t>(by_type, type, null_handling);
	case PhysicalType::INT128:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, hugeint_t>(by_type, type, null_handling);
	case PhysicalType::DOUBLE:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, double>(by_type, type, null_handling);
	case PhysicalType::VARCHAR:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, string_t>(by_type, type, null_handling);
	default:
		throw InternalException("Unimplemented arg_min/arg_max by aggregate");
	}
}
#endif

template <class OP, class ARG_TYPE>
void AddArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type, ArgMinMaxNullHandling null_handling) {
	auto by_types = ArgMaxByTypes();
	for (const auto &by_type : by_types) {
#ifndef DUCKDB_SMALLER_BINARY
		fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(by_type, type, null_handling));
#else
		fun.AddFunction(GetArgMinMaxFunctionInternal<OP, string_t, string_t>(by_type, type, null_handling));
#endif
	}
}

template <class OP>
AggregateFunction GetDecimalArgMinMaxFunction(const LogicalType &by_type, const LogicalType &type,
                                              ArgMinMaxNullHandling null_handling) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
#ifndef DUCKDB_SMALLER_BINARY
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return GetArgMinMaxFunctionBy<OP, int16_t>(by_type, type, null_handling);
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionBy<OP, int32_t>(by_type, type, null_handling);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionBy<OP, int64_t>(by_type, type, null_handling);
	default:
		return GetArgMinMaxFunctionBy<OP, hugeint_t>(by_type, type, null_handling);
	}
#else
	return GetArgMinMaxFunctionInternal<OP, string_t, string_t>(by_type, type, null_handling);
#endif
}

template <class OP, ArgMinMaxNullHandling NULL_HANDLING>
unique_ptr<FunctionData> BindDecimalArgMinMax(ClientContext &context, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	auto by_type = arguments[1]->return_type;

	// To avoid a combinatorial explosion, cast the ordering argument to one from the list
	auto by_types = ArgMaxByTypes();
	idx_t best_target = DConstants::INVALID_INDEX;
	int64_t lowest_cost = NumericLimits<int64_t>::Maximum();
	for (idx_t i = 0; i < by_types.size(); ++i) {
		// Before falling back to casting, check for a physical type match for the by_type
		if (by_types[i].InternalType() == by_type.InternalType()) {
			lowest_cost = 0;
			best_target = DConstants::INVALID_INDEX;
			break;
		}

		auto cast_cost = CastFunctionSet::ImplicitCastCost(context, by_type, by_types[i]);
		if (cast_cost < 0) {
			continue;
		}
		if (cast_cost < lowest_cost) {
			best_target = i;
		}
	}

	if (best_target != DConstants::INVALID_INDEX) {
		by_type = by_types[best_target];
	}

	auto name = std::move(function.name);
	function = GetDecimalArgMinMaxFunction<OP>(by_type, decimal_type, NULL_HANDLING);
	function.name = std::move(name);
	function.SetReturnType(decimal_type);

	auto function_data = make_uniq<ArgMinMaxFunctionData>(NULL_HANDLING);
	return unique_ptr<FunctionData>(std::move(function_data));
}

template <class OP>
void AddDecimalArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &by_type,
                                   const ArgMinMaxNullHandling null_handling) {
	switch (null_handling) {
	case ArgMinMaxNullHandling::IGNORE_ANY_NULL:
		fun.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, by_type}, LogicalTypeId::DECIMAL, nullptr, nullptr,
		                                  nullptr, nullptr, nullptr, nullptr,
		                                  BindDecimalArgMinMax<OP, ArgMinMaxNullHandling::IGNORE_ANY_NULL>));
		break;
	case ArgMinMaxNullHandling::HANDLE_ARG_NULL:
		fun.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, by_type}, LogicalTypeId::DECIMAL, nullptr, nullptr,
		                                  nullptr, nullptr, nullptr, nullptr,
		                                  BindDecimalArgMinMax<OP, ArgMinMaxNullHandling::HANDLE_ARG_NULL>));
		break;
	case ArgMinMaxNullHandling::HANDLE_ANY_NULL:
		fun.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, by_type}, LogicalTypeId::DECIMAL, nullptr, nullptr,
		                                  nullptr, nullptr, nullptr, nullptr,
		                                  BindDecimalArgMinMax<OP, ArgMinMaxNullHandling::HANDLE_ANY_NULL>));
		break;
	}
}

template <class OP>
void AddGenericArgMinMaxFunction(AggregateFunctionSet &fun, const ArgMinMaxNullHandling null_handling) {
	fun.AddFunction(GetGenericArgMinMaxFunction<OP>(null_handling));
}

template <class COMPARATOR, OrderType ORDER_TYPE>
void AddArgMinMaxFunctions(AggregateFunctionSet &fun, const ArgMinMaxNullHandling null_handling) {
	using GENERIC_VECTOR_OP = VectorArgMinMaxBase<LessThan, ORDER_TYPE, GenericArgMinMaxState<ORDER_TYPE>>;
#ifndef DUCKDB_SMALLER_BINARY
	using OP = ArgMinMaxBase<COMPARATOR>;
	using VECTOR_OP = VectorArgMinMaxBase<COMPARATOR, ORDER_TYPE>;
#else
	using OP = GENERIC_VECTOR_OP;
	using VECTOR_OP = GENERIC_VECTOR_OP;
#endif
	AddArgMinMaxFunctionBy<OP, int32_t>(fun, LogicalType::INTEGER, null_handling);
	AddArgMinMaxFunctionBy<OP, int64_t>(fun, LogicalType::BIGINT, null_handling);
	AddArgMinMaxFunctionBy<OP, double>(fun, LogicalType::DOUBLE, null_handling);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::VARCHAR, null_handling);
	AddArgMinMaxFunctionBy<OP, date_t>(fun, LogicalType::DATE, null_handling);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP, null_handling);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP_TZ, null_handling);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::BLOB, null_handling);

	auto by_types = ArgMaxByTypes();
	for (const auto &by_type : by_types) {
		AddDecimalArgMinMaxFunctionBy<OP>(fun, by_type, null_handling);
	}

	AddVectorArgMinMaxFunctionBy<VECTOR_OP, string_t>(fun, LogicalType::ANY, null_handling);

	// we always use LessThan when using sort keys because the ORDER_TYPE takes care of selecting the lowest or highest
	AddGenericArgMinMaxFunction<GENERIC_VECTOR_OP>(fun, null_handling);
}

//------------------------------------------------------------------------------
// ArgMinMax(N) Function
//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
// State
//------------------------------------------------------------------------------

template <class A, class B, class COMPARATOR>
class ArgMinMaxNState {
public:
	using VAL_TYPE = A;
	using ARG_TYPE = B;

	using V = typename VAL_TYPE::TYPE;
	using K = typename ARG_TYPE::TYPE;

	BinaryAggregateHeap<K, V, COMPARATOR> heap;

	bool is_initialized = false;
	void Initialize(ArenaAllocator &allocator, idx_t nval) {
		heap.Initialize(allocator, nval);
		is_initialized = true;
	}
};

//------------------------------------------------------------------------------
// Operation
//------------------------------------------------------------------------------
template <class STATE>
void ArgMinMaxNUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
                      idx_t count) {
	D_ASSERT(aggr_input.bind_data);
	const auto &bind_data = aggr_input.bind_data->Cast<ArgMinMaxFunctionData>();

	auto &val_vector = inputs[0];
	auto &arg_vector = inputs[1];
	auto &n_vector = inputs[2];

	UnifiedVectorFormat val_format;
	UnifiedVectorFormat arg_format;
	UnifiedVectorFormat n_format;
	UnifiedVectorFormat state_format;

	auto val_extra_state = STATE::VAL_TYPE::CreateExtraState(val_vector, count);
	auto arg_extra_state = STATE::ARG_TYPE::CreateExtraState(arg_vector, count);

	STATE::VAL_TYPE::PrepareData(val_vector, count, val_extra_state, val_format, bind_data.nulls_last);
	STATE::ARG_TYPE::PrepareData(arg_vector, count, arg_extra_state, arg_format, bind_data.nulls_last);

	n_vector.ToUnifiedFormat(count, n_format);
	state_vector.ToUnifiedFormat(count, state_format);

	auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);

	for (idx_t i = 0; i < count; i++) {
		const auto arg_idx = arg_format.sel->get_index(i);
		const auto val_idx = val_format.sel->get_index(i);

		if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL &&
		    (!arg_format.validity.RowIsValid(arg_idx) || !val_format.validity.RowIsValid(val_idx))) {
			continue;
		}
		if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ARG_NULL &&
		    !val_format.validity.RowIsValid(val_idx)) {
			continue;
		}

		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];

		// Initialize the heap if necessary and add the input to the heap
		if (!state.is_initialized) {
			static constexpr int64_t MAX_N = 1000000;
			const auto nidx = n_format.sel->get_index(i);
			if (!n_format.validity.RowIsValid(nidx)) {
				throw InvalidInputException("Invalid input for arg_min/arg_max: n value cannot be NULL");
			}
			const auto nval = UnifiedVectorFormat::GetData<int64_t>(n_format)[nidx];
			if (nval <= 0) {
				throw InvalidInputException("Invalid input for arg_min/arg_max: n value must be > 0");
			}
			if (nval >= MAX_N) {
				throw InvalidInputException("Invalid input for arg_min/arg_max: n value must be < %d", MAX_N);
			}
			state.Initialize(aggr_input.allocator, UnsafeNumericCast<idx_t>(nval));
		}

		// Now add the input to the heap
		auto arg_val = STATE::ARG_TYPE::Create(arg_format, arg_idx);
		auto val_val = STATE::VAL_TYPE::Create(val_format, val_idx);

		state.heap.Insert(aggr_input.allocator, arg_val, val_val);
	}
}

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
template <class VAL_TYPE, class ARG_TYPE, class COMPARATOR>
void SpecializeArgMinMaxNFunction(AggregateFunction &function) {
	using STATE = ArgMinMaxNState<VAL_TYPE, ARG_TYPE, COMPARATOR>;
	using OP = MinMaxNOperation;

	function.state_size = AggregateFunction::StateSize<STATE>;
	function.initialize = AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>;
	function.combine = AggregateFunction::StateCombine<STATE, OP>;
	function.destructor = AggregateFunction::StateDestroy<STATE, OP>;

	function.finalize = MinMaxNOperation::Finalize<STATE>;
	function.update = ArgMinMaxNUpdate<STATE>;
}

template <class VAL_TYPE, class COMPARATOR>
void SpecializeArgMinMaxNFunction(PhysicalType arg_type, AggregateFunction &function) {
	switch (arg_type) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxStringValue, COMPARATOR>(function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<int32_t>, COMPARATOR>(function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<int64_t>, COMPARATOR>(function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<float>, COMPARATOR>(function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<double>, COMPARATOR>(function);
		break;
#endif
	default:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	}
}

template <class COMPARATOR>
void SpecializeArgMinMaxNFunction(PhysicalType val_type, PhysicalType arg_type, AggregateFunction &function) {
	switch (val_type) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNFunction<MinMaxStringValue, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<int32_t>, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<int64_t>, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<float>, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<double>, COMPARATOR>(arg_type, function);
		break;
#endif
	default:
		SpecializeArgMinMaxNFunction<MinMaxFallbackValue, COMPARATOR>(arg_type, function);
		break;
	}
}

template <class VAL_TYPE, class ARG_TYPE, class COMPARATOR>
void SpecializeArgMinMaxNullNFunction(AggregateFunction &function) {
	using STATE = ArgMinMaxNState<VAL_TYPE, ARG_TYPE, COMPARATOR>;
	using OP = MinMaxNOperation;

	function.state_size = AggregateFunction::StateSize<STATE>;
	function.initialize = AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>;
	function.combine = AggregateFunction::StateCombine<STATE, OP>;
	function.destructor = AggregateFunction::StateDestroy<STATE, OP>;

	function.finalize = MinMaxNOperation::Finalize<STATE>;
	function.update = ArgMinMaxNUpdate<STATE>;
}

template <class VAL_TYPE, bool NULLS_LAST, class COMPARATOR>
void SpecializeArgMinMaxNullNFunction(PhysicalType arg_type, AggregateFunction &function) {
	switch (arg_type) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<int32_t, NULLS_LAST>, COMPARATOR>(function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<int64_t, NULLS_LAST>, COMPARATOR>(function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<float, NULLS_LAST>, COMPARATOR>(function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<double, NULLS_LAST>, COMPARATOR>(function);
		break;
#endif
	default:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	}
}

template <bool NULLS_LAST, class COMPARATOR>
void SpecializeArgMinMaxNullNFunction(PhysicalType val_type, PhysicalType arg_type, AggregateFunction &function) {
	switch (val_type) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNullNFunction<MinMaxFallbackValue, NULLS_LAST, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<int32_t, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                      function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<int64_t, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                      function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<float, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                    function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<double, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                     function);
		break;
#endif
	default:
		SpecializeArgMinMaxNullNFunction<MinMaxFallbackValue, NULLS_LAST, COMPARATOR>(arg_type, function);
		break;
	}
}

template <ArgMinMaxNullHandling NULL_HANDLING, bool NULLS_LAST, class COMPARATOR>
unique_ptr<FunctionData> ArgMinMaxNBind(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	const auto val_type = arguments[0]->return_type.InternalType();
	const auto arg_type = arguments[1]->return_type.InternalType();
	function.SetReturnType(LogicalType::LIST(arguments[0]->return_type));

	// Specialize the function based on the input types
	auto function_data = make_uniq<ArgMinMaxFunctionData>(NULL_HANDLING, NULLS_LAST);
	if (NULL_HANDLING != ArgMinMaxNullHandling::IGNORE_ANY_NULL) {
		SpecializeArgMinMaxNullNFunction<NULLS_LAST, COMPARATOR>(val_type, arg_type, function);
	} else {
		SpecializeArgMinMaxNFunction<COMPARATOR>(val_type, arg_type, function);
	}

	return unique_ptr<FunctionData>(std::move(function_data));
}

template <ArgMinMaxNullHandling NULL_HANDLING, bool NULLS_LAST, class COMPARATOR>
void AddArgMinMaxNFunction(AggregateFunctionSet &set) {
	AggregateFunction function({LogicalTypeId::ANY, LogicalTypeId::ANY, LogicalType::BIGINT},
	                           LogicalType::LIST(LogicalType::ANY), nullptr, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, ArgMinMaxNBind<NULL_HANDLING, NULLS_LAST, COMPARATOR>);

	return set.AddFunction(function);
}

} // namespace

//------------------------------------------------------------------------------
// Function Registration
//------------------------------------------------------------------------------

AggregateFunctionSet ArgMinFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, OrderType::ASCENDING>(fun, ArgMinMaxNullHandling::IGNORE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::IGNORE_ANY_NULL, true, LessThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, OrderType::DESCENDING>(fun, ArgMinMaxNullHandling::IGNORE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::IGNORE_ANY_NULL, false, GreaterThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMinNullFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, OrderType::ASCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ARG_NULL);
	return fun;
}

AggregateFunctionSet ArgMaxNullFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, OrderType::DESCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ARG_NULL);
	return fun;
}

AggregateFunctionSet ArgMinNullsLastFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, OrderType::ASCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::HANDLE_ANY_NULL, true, LessThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxNullsLastFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, OrderType::DESCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::HANDLE_ANY_NULL, false, GreaterThan>(fun);
	return fun;
}

} // namespace duckdb
