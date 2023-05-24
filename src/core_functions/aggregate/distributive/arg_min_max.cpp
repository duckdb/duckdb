#include "duckdb/core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

struct ArgMinMaxStateBase {
	ArgMinMaxStateBase() : is_initialized(false) {
	}

	template <class T>
	static inline void CreateValue(T &value) {
	}

	template <class T>
	static inline void DestroyValue(T &value) {
	}

	template <class T>
	static inline void AssignValue(T &target, T new_value, bool is_initialized) {
		target = new_value;
	}

	template <typename T>
	static inline void ReadValue(Vector &result, T &arg, T &target) {
		target = arg;
	}

	bool is_initialized;
};

// Out-of-line specialisations
template <>
void ArgMinMaxStateBase::CreateValue(Vector *&value) {
	value = nullptr;
}

template <>
void ArgMinMaxStateBase::DestroyValue(string_t &value) {
	if (!value.IsInlined()) {
		delete[] value.GetData();
	}
}

template <>
void ArgMinMaxStateBase::DestroyValue(Vector *&value) {
	delete value;
	value = nullptr;
}

template <>
void ArgMinMaxStateBase::AssignValue(string_t &target, string_t new_value, bool is_initialized) {
	if (is_initialized) {
		DestroyValue(target);
	}
	if (new_value.IsInlined()) {
		target = new_value;
	} else {
		// non-inlined string, need to allocate space for it
		auto len = new_value.GetSize();
		auto ptr = new char[len];
		memcpy(ptr, new_value.GetData(), len);

		target = string_t(ptr, len);
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

	~ArgMinMaxState() {
		if (is_initialized) {
			DestroyValue(arg);
			DestroyValue(value);
			is_initialized = false;
		}
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

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &x, const B_TYPE &y, AggregateBinaryInput &) {
		if (!state.is_initialized) {
			STATE::template AssignValue<A_TYPE>(state.arg, x, false);
			STATE::template AssignValue<B_TYPE>(state.value, y, false);
			state.is_initialized = true;
		} else {
			OP::template Execute<A_TYPE, B_TYPE, STATE>(state, x, y);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Execute(STATE &state, A_TYPE x_data, B_TYPE y_data) {
		if (COMPARATOR::Operation(y_data, state.value)) {
			STATE::template AssignValue<A_TYPE>(state.arg, x_data, true);
			STATE::template AssignValue<B_TYPE>(state.value, y_data, true);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_initialized) {
			return;
		}
		if (!target.is_initialized || COMPARATOR::Operation(source.value, target.value)) {
			STATE::template AssignValue(target.arg, source.arg, target.is_initialized);
			STATE::template AssignValue(target.value, source.value, target.is_initialized);
			target.is_initialized = true;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized) {
			finalize_data.ReturnNull();
		} else {
			STATE::template ReadValue(finalize_data.result, state.arg, target);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <typename COMPARATOR>
struct VectorArgMinMaxBase : ArgMinMaxBase<COMPARATOR> {
	template <class STATE>
	static void AssignVector(STATE &state, Vector &arg, const idx_t idx) {
		if (!state.is_initialized) {
			state.arg = new Vector(arg.GetType());
			state.arg->SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		sel_t selv = idx;
		SelectionVector sel(&selv);
		VectorOperations::Copy(arg, *state.arg, sel, 1, 0, 0);
	}

	template <class STATE>
	static void Update(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector, idx_t count) {
		auto &arg = inputs[0];
		UnifiedVectorFormat adata;
		arg.ToUnifiedFormat(count, adata);

		using BY_TYPE = typename STATE::BY_TYPE;
		auto &by = inputs[1];
		UnifiedVectorFormat bdata;
		by.ToUnifiedFormat(count, bdata);
		const auto bys = UnifiedVectorFormat::GetData<BY_TYPE>(bdata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		auto states = (STATE **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			const auto bidx = bdata.sel->get_index(i);
			if (!bdata.validity.RowIsValid(bidx)) {
				continue;
			}
			const auto bval = bys[bidx];

			const auto sidx = sdata.sel->get_index(i);
			auto &state = *states[sidx];
			if (!state.is_initialized) {
				STATE::template AssignValue<BY_TYPE>(state.value, bval, false);
				AssignVector(state, arg, i);
				state.is_initialized = true;

			} else if (COMPARATOR::template Operation<BY_TYPE>(bval, state.value)) {
				STATE::template AssignValue<BY_TYPE>(state.value, bval, true);
				AssignVector(state, arg, i);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_initialized) {
			return;
		}
		if (!target.is_initialized || COMPARATOR::Operation(source.value, target.value)) {
			STATE::template AssignValue(target.value, source.value, target.is_initialized);
			AssignVector(target, *source.arg, 0);
			target.is_initialized = true;
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized) {
			finalize_data.ReturnNull();
		} else {
			VectorOperations::Copy(*state.arg, finalize_data.result, 1, 0, finalize_data.result_idx);
		}
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function.arguments[0] = arguments[0]->return_type;
		function.return_type = arguments[0]->return_type;
		return nullptr;
	}
};

template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunction GetVectorArgMinMaxFunctionInternal(const LogicalType &by_type, const LogicalType &type) {
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	return AggregateFunction(
	    {type, by_type}, type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    OP::template Update<STATE>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, OP::Bind, AggregateFunction::StateDestroy<STATE, OP>);
}

template <class OP, class ARG_TYPE>
AggregateFunction GetVectorArgMinMaxFunctionBy(const LogicalType &by_type, const LogicalType &type) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, int32_t>(by_type, type);
	case PhysicalType::INT64:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, int64_t>(by_type, type);
	case PhysicalType::DOUBLE:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, double>(by_type, type);
	case PhysicalType::VARCHAR:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, string_t>(by_type, type);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}

template <class OP, class ARG_TYPE>
void AddVectorArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type) {
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::INTEGER, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BIGINT, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DOUBLE, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::VARCHAR, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DATE, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP_TZ, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BLOB, type));
}

template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunction GetArgMinMaxFunctionInternal(const LogicalType &by_type, const LogicalType &type) {
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	auto function = AggregateFunction::BinaryAggregate<STATE, ARG_TYPE, BY_TYPE, ARG_TYPE, OP>(type, by_type, type);
	if (type.InternalType() == PhysicalType::VARCHAR || by_type.InternalType() == PhysicalType::VARCHAR) {
		function.destructor = AggregateFunction::StateDestroy<STATE, OP>;
	}
	return function;
}

template <class OP, class ARG_TYPE>
AggregateFunction GetArgMinMaxFunctionBy(const LogicalType &by_type, const LogicalType &type) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int32_t>(by_type, type);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int64_t>(by_type, type);
	case PhysicalType::DOUBLE:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, double>(by_type, type);
	case PhysicalType::VARCHAR:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, string_t>(by_type, type);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}

template <class OP, class ARG_TYPE>
void AddArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type) {
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::INTEGER, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BIGINT, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DOUBLE, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::VARCHAR, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DATE, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP_TZ, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BLOB, type));
}

template <class COMPARATOR>
static void AddArgMinMaxFunctions(AggregateFunctionSet &fun) {
	using OP = ArgMinMaxBase<COMPARATOR>;
	AddArgMinMaxFunctionBy<OP, int32_t>(fun, LogicalType::INTEGER);
	AddArgMinMaxFunctionBy<OP, int64_t>(fun, LogicalType::BIGINT);
	AddArgMinMaxFunctionBy<OP, double>(fun, LogicalType::DOUBLE);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::VARCHAR);
	AddArgMinMaxFunctionBy<OP, date_t>(fun, LogicalType::DATE);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP_TZ);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::BLOB);

	using VECTOR_OP = VectorArgMinMaxBase<COMPARATOR>;
	AddVectorArgMinMaxFunctionBy<VECTOR_OP, Vector *>(fun, LogicalType::ANY);
}

AggregateFunctionSet ArgMinFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan>(fun);
	return fun;
}

} // namespace duckdb
