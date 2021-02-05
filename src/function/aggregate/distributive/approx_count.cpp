#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
namespace duckdb {

struct approx_distinct_count_state_t {
	HyperLogLog*  log;
};

struct ApproxCountDistinctFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->log = new HyperLogLog();
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		auto new_log = target->log->Merge_P(*source.log);
		D_ASSERT(target->log);
		D_ASSERT(source.log);
		delete target->log;
		target->log = new_log;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		target[idx] = state->log->Count();

	}

		template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		    if (nullmask[idx]) {
			return;
		}
		    INPUT_TYPE value = input[idx];
		state->log->Add((uint8_t *)&value, sizeof(value));
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->log) {
			delete state->log;
		}
	}
};


AggregateFunction GetApproxCountDistinctFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT16:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, uint16_t , int64_t, ApproxCountDistinctFunction>(LogicalType::UTINYINT,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::UINT32:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, uint32_t , int64_t, ApproxCountDistinctFunction>(LogicalType::UINTEGER,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::UINT64:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, uint64_t , int64_t, ApproxCountDistinctFunction>(LogicalType::UBIGINT,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, int16_t , int64_t, ApproxCountDistinctFunction>(LogicalType::TINYINT,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, int32_t , int64_t, ApproxCountDistinctFunction>(LogicalType::INTEGER,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, int64_t , int64_t, ApproxCountDistinctFunction>(LogicalType::BIGINT,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, float , int64_t, ApproxCountDistinctFunction>(LogicalType::FLOAT,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<approx_distinct_count_state_t, double , int64_t, ApproxCountDistinctFunction>(LogicalType::DOUBLE,
		                                                                                     LogicalType::BIGINT);

	default:
		throw NotImplementedException("Unimplemented quantile aggregate");
	}
}

void ApproxCountDistinctFun::RegisterFunction(BuiltinFunctions &set) {

	AggregateFunctionSet approx_count("approx_count_distinct");
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::UINT16));
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::UINT32));
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::UINT64));
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::FLOAT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::INT16));
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::INT32));
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::INT64));
	approx_count.AddFunction(GetApproxCountDistinctFunction(PhysicalType::DOUBLE));
	set.AddFunction(approx_count);
}

} // namespace duckdb
