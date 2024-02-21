#include "duckdb/core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

struct ArgMinMaxStateBase {
	ArgMinMaxStateBase() : is_initialized(false), arg_null(false) {
	}

	template <class T>
	static inline void CreateValue(T &value) {
	}

	template <class T>
	static inline void DestroyValue(T &value) {
	}

	template <class T>
	static inline void AssignValue(T &target, T new_value) {
		target = new_value;
	}

	template <typename T>
	static inline void ReadValue(Vector &result, T &arg, T &target) {
		target = arg;
	}

	bool is_initialized;
	bool arg_null;
};

// Out-of-line specialisations
template <>
void ArgMinMaxStateBase::CreateValue(string_t &value) {
	value = string_t(uint32_t(0));
}

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
void ArgMinMaxStateBase::AssignValue(string_t &target, string_t new_value) {
	DestroyValue(target);
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

template <class COMPARATOR, bool IGNORE_NULL>
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
	static void Assign(STATE &state, const A_TYPE &x, const B_TYPE &y, const bool x_null) {
		if (IGNORE_NULL) {
			STATE::template AssignValue<A_TYPE>(state.arg, x);
			STATE::template AssignValue<B_TYPE>(state.value, y);
		} else {
			state.arg_null = x_null;
			if (!state.arg_null) {
				STATE::template AssignValue<A_TYPE>(state.arg, x);
			}
			STATE::template AssignValue<B_TYPE>(state.value, y);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &x, const B_TYPE &y, AggregateBinaryInput &binary) {
		if (!state.is_initialized) {
			if (IGNORE_NULL || binary.right_mask.RowIsValid(binary.ridx)) {
				Assign(state, x, y, !binary.left_mask.RowIsValid(binary.lidx));
				state.is_initialized = true;
			}
		} else {
			OP::template Execute<A_TYPE, B_TYPE, STATE>(state, x, y, binary);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Execute(STATE &state, A_TYPE x_data, B_TYPE y_data, AggregateBinaryInput &binary) {
		if ((IGNORE_NULL || binary.right_mask.RowIsValid(binary.ridx)) && COMPARATOR::Operation(y_data, state.value)) {
			Assign(state, x_data, y_data, !binary.left_mask.RowIsValid(binary.lidx));
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_initialized) {
			return;
		}
		if (!target.is_initialized || COMPARATOR::Operation(source.value, target.value)) {
			Assign(target, source.arg, source.value, source.arg_null);
			target.is_initialized = true;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized || state.arg_null) {
			finalize_data.ReturnNull();
		} else {
			STATE::template ReadValue(finalize_data.result, state.arg, target);
		}
	}

	static bool IgnoreNull() {
		return IGNORE_NULL;
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		ExpressionBinder::PushCollation(context, arguments[1], arguments[1]->return_type, false);
		function.arguments[0] = arguments[0]->return_type;
		function.return_type = arguments[0]->return_type;
		return nullptr;
	}
};

template <typename COMPARATOR, bool IGNORE_NULL>
struct VectorArgMinMaxBase : ArgMinMaxBase<COMPARATOR, IGNORE_NULL> {
	template <class STATE>
	static void AssignVector(STATE &state, Vector &arg, bool arg_null, const idx_t idx) {
		if (!state.arg) {
			state.arg = new Vector(arg.GetType(), 1);
			state.arg->SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		state.arg_null = arg_null;
		if (!arg_null) {
			sel_t selv = idx;
			SelectionVector sel(&selv);
			VectorOperations::Copy(arg, *state.arg, sel, 1, 0, 0);
		}
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

			const auto aidx = adata.sel->get_index(i);
			const auto arg_null = !adata.validity.RowIsValid(aidx);
			if (IGNORE_NULL && arg_null) {
				continue;
			}

			const auto sidx = sdata.sel->get_index(i);
			auto &state = *states[sidx];
			if (!state.is_initialized) {
				STATE::template AssignValue<BY_TYPE>(state.value, bval);
				AssignVector(state, arg, arg_null, i);
				state.is_initialized = true;

			} else if (COMPARATOR::template Operation<BY_TYPE>(bval, state.value)) {
				STATE::template AssignValue<BY_TYPE>(state.value, bval);
				AssignVector(state, arg, arg_null, i);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_initialized) {
			return;
		}
		if (!target.is_initialized || COMPARATOR::Operation(source.value, target.value)) {
			STATE::template AssignValue(target.value, source.value);
			AssignVector(target, *source.arg, source.arg_null, 0);
			target.is_initialized = true;
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized || state.arg_null) {
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

static const vector<LogicalType> ArgMaxByTypes() {
	vector<LogicalType> types = {LogicalType::INTEGER,      LogicalType::BIGINT, LogicalType::DOUBLE,
	                             LogicalType::VARCHAR,      LogicalType::DATE,   LogicalType::TIMESTAMP,
	                             LogicalType::TIMESTAMP_TZ, LogicalType::BLOB};
	return types;
}

template <class OP, class ARG_TYPE>
void AddVectorArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type) {
	auto by_types = ArgMaxByTypes();
	for (const auto &by_type : by_types) {
		fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(by_type, type));
	}
}

template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunction GetArgMinMaxFunctionInternal(const LogicalType &by_type, const LogicalType &type) {
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	auto function = AggregateFunction::BinaryAggregate<STATE, ARG_TYPE, BY_TYPE, ARG_TYPE, OP>(type, by_type, type);
	if (type.InternalType() == PhysicalType::VARCHAR || by_type.InternalType() == PhysicalType::VARCHAR) {
		function.destructor = AggregateFunction::StateDestroy<STATE, OP>;
	}
	if (by_type.InternalType() == PhysicalType::VARCHAR) {
		function.bind = OP::Bind;
	}
	return function;
}

template <class OP, class ARG_TYPE>
AggregateFunction GetArgMinMaxFunctionBy(const LogicalType &by_type, const LogicalType &type) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT8:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int8_t>(by_type, type);
	case PhysicalType::INT16:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int16_t>(by_type, type);
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int32_t>(by_type, type);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int64_t>(by_type, type);
	case PhysicalType::FLOAT:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, float>(by_type, type);
	case PhysicalType::DOUBLE:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, double>(by_type, type);
	case PhysicalType::VARCHAR:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, string_t>(by_type, type);
	default:
		throw InternalException("Unimplemented arg_min/arg_max by aggregate");
	}
}

template <class OP, class ARG_TYPE>
void AddArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type) {
	auto by_types = ArgMaxByTypes();
	for (const auto &by_type : by_types) {
		fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(by_type, type));
	}
}

template <class OP>
static AggregateFunction GetDecimalArgMinMaxFunction(const LogicalType &by_type, const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return GetArgMinMaxFunctionBy<OP, int16_t>(by_type, type);
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionBy<OP, int32_t>(by_type, type);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionBy<OP, int64_t>(by_type, type);
	default:
		return GetArgMinMaxFunctionBy<OP, hugeint_t>(by_type, type);
	}
}

template <class OP>
static unique_ptr<FunctionData> BindDecimalArgMinMax(ClientContext &context, AggregateFunction &function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	auto by_type = arguments[1]->return_type;
	auto name = std::move(function.name);
	function = GetDecimalArgMinMaxFunction<OP>(by_type, decimal_type);
	function.name = std::move(name);
	function.return_type = decimal_type;
	return nullptr;
}

template <class OP>
void AddDecimalArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &by_type) {
	fun.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, by_type}, LogicalTypeId::DECIMAL, nullptr, nullptr,
	                                  nullptr, nullptr, nullptr, nullptr, BindDecimalArgMinMax<OP>));
}

template <class COMPARATOR, bool IGNORE_NULL>
static void AddArgMinMaxFunctions(AggregateFunctionSet &fun) {
	using OP = ArgMinMaxBase<COMPARATOR, IGNORE_NULL>;
	AddArgMinMaxFunctionBy<OP, int32_t>(fun, LogicalType::INTEGER);
	AddArgMinMaxFunctionBy<OP, int64_t>(fun, LogicalType::BIGINT);
	AddArgMinMaxFunctionBy<OP, double>(fun, LogicalType::DOUBLE);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::VARCHAR);
	AddArgMinMaxFunctionBy<OP, date_t>(fun, LogicalType::DATE);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP_TZ);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::BLOB);

	auto by_types = ArgMaxByTypes();
	for (const auto &by_type : by_types) {
		AddDecimalArgMinMaxFunctionBy<OP>(fun, by_type);
	}

	using VECTOR_OP = VectorArgMinMaxBase<COMPARATOR, IGNORE_NULL>;
	AddVectorArgMinMaxFunctionBy<VECTOR_OP, Vector *>(fun, LogicalType::ANY);
}

AggregateFunctionSet ArgMinFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, true>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, true>(fun);
	return fun;
}

AggregateFunctionSet ArgMinNullFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, false>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxNullFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, false>(fun);
	return fun;
}

} // namespace duckdb
