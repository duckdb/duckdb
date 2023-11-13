#include "duckdb/core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct ApproxDistinctCountState {
	ApproxDistinctCountState() : log(nullptr) {
	}
	~ApproxDistinctCountState() {
		if (log) {
			delete log;
		}
	}

	HyperLogLog *log;
};

struct ApproxCountDistinctFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.log = nullptr;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.log) {
			return;
		}
		if (!target.log) {
			target.log = new HyperLogLog();
		}
		D_ASSERT(target.log);
		D_ASSERT(source.log);
		auto new_log = target.log->MergePointer(*source.log);
		delete target.log;
		target.log = new_log;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.log) {
			target = state.log->Count();
		} else {
			target = 0;
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.log) {
			delete state.log;
			state.log = nullptr;
		}
	}
};

static void ApproxCountDistinctSimpleUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                                    data_ptr_t state, idx_t count) {
	D_ASSERT(input_count == 1);

	auto agg_state = reinterpret_cast<ApproxDistinctCountState *>(state);
	if (!agg_state->log) {
		agg_state->log = new HyperLogLog();
	}

	UnifiedVectorFormat vdata;
	inputs[0].ToUnifiedFormat(count, vdata);

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];
	HyperLogLog::ProcessEntries(vdata, inputs[0].GetType(), indices, counts, count);
	agg_state->log->AddToLog(vdata, count, indices, counts);
}

static void ApproxCountDistinctUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                              Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetDataNoConst<ApproxDistinctCountState *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto agg_state = states[sdata.sel->get_index(i)];
		if (!agg_state->log) {
			agg_state->log = new HyperLogLog();
		}
	}

	UnifiedVectorFormat vdata;
	inputs[0].ToUnifiedFormat(count, vdata);

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];
	HyperLogLog::ProcessEntries(vdata, inputs[0].GetType(), indices, counts, count);
	HyperLogLog::AddToLogs(vdata, count, indices, counts, reinterpret_cast<HyperLogLog ***>(states), sdata.sel);
}

AggregateFunction GetApproxCountDistinctFunction(const LogicalType &input_type) {
	auto fun = AggregateFunction(
	    {input_type}, LogicalTypeId::BIGINT, AggregateFunction::StateSize<ApproxDistinctCountState>,
	    AggregateFunction::StateInitialize<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    ApproxCountDistinctUpdateFunction,
	    AggregateFunction::StateCombine<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    AggregateFunction::StateFinalize<ApproxDistinctCountState, int64_t, ApproxCountDistinctFunction>,
	    ApproxCountDistinctSimpleUpdateFunction, nullptr,
	    AggregateFunction::StateDestroy<ApproxDistinctCountState, ApproxCountDistinctFunction>);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

AggregateFunctionSet ApproxCountDistinctFun::GetFunctions() {
	AggregateFunctionSet approx_count("approx_count_distinct");
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UTINYINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::USMALLINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UINTEGER));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UBIGINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TINYINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::SMALLINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::BIGINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::HUGEINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::FLOAT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::DOUBLE));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::VARCHAR));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TIMESTAMP));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TIMESTAMP_TZ));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::BLOB));
	return approx_count;
}

} // namespace duckdb
