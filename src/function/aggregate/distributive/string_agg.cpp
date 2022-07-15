#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

struct StringAggState {
	idx_t size;
	idx_t alloc_size;
	char *dataptr;
};

struct StringAggBindData : public FunctionData {
	explicit StringAggBindData(string sep_p) : sep(move(sep_p)) {
	}

	string sep;

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<StringAggBindData>(sep);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = (StringAggBindData &)other_p;
		return sep == other.sep;
	}
};

struct StringAggFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->dataptr = nullptr;
		state->alloc_size = 0;
		state->size = 0;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->dataptr) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = StringVector::AddString(result, state->dataptr, state->size);
		}
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->dataptr) {
			delete[] state->dataptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	static inline void PerformOperation(StringAggState *state, const char *str, const char *sep, idx_t str_size,
	                                    idx_t sep_size) {
		if (!state->dataptr) {
			// first iteration: allocate space for the string and copy it into the state
			state->alloc_size = MaxValue<idx_t>(8, NextPowerOfTwo(str_size));
			state->dataptr = new char[state->alloc_size];
			state->size = str_size;
			memcpy(state->dataptr, str, str_size);
		} else {
			// subsequent iteration: first check if we have space to place the string and separator
			idx_t required_size = state->size + str_size + sep_size;
			if (required_size > state->alloc_size) {
				// no space! allocate extra space
				while (state->alloc_size < required_size) {
					state->alloc_size *= 2;
				}
				auto new_data = new char[state->alloc_size];
				memcpy(new_data, state->dataptr, state->size);
				delete[] state->dataptr;
				state->dataptr = new_data;
			}
			// copy the separator
			memcpy(state->dataptr + state->size, sep, sep_size);
			state->size += sep_size;
			// copy the string
			memcpy(state->dataptr + state->size, str, str_size);
			state->size += str_size;
		}
	}

	static inline void PerformOperation(StringAggState *state, string_t str, FunctionData *data_p) {
		auto &data = (StringAggBindData &)*data_p;
		PerformOperation(state, str.GetDataUnsafe(), data.sep.c_str(), str.GetSize(), data.sep.size());
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *str_data,
	                      ValidityMask &str_mask, idx_t str_idx) {
		PerformOperation(state, str_data[str_idx], aggr_input_data.bind_data);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &aggr_input_data) {
		if (!source.dataptr) {
			// source is not set: skip combining
			return;
		}
		PerformOperation(target, string_t(source.dataptr, source.size), aggr_input_data.bind_data);
	}
};

unique_ptr<FunctionData> StringAggBind(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 1) {
		// single argument: default to comma
		return make_unique<StringAggBindData>(",");
	}
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("Separator argument to StringAgg must be a constant");
	}
	auto separator_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	if (separator_val.IsNull()) {
		arguments[0] = make_unique<BoundConstantExpression>(Value(LogicalType::VARCHAR));
	}
	function.arguments.erase(function.arguments.begin() + 1);
	return make_unique<StringAggBindData>(separator_val.ToString());
}

void StringAggFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet string_agg("string_agg");
	string_agg.AddFunction(
	    AggregateFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      AggregateFunction::StateSize<StringAggState>,
	                      AggregateFunction::StateInitialize<StringAggState, StringAggFunction>,
	                      AggregateFunction::UnaryScatterUpdate<StringAggState, string_t, StringAggFunction>,
	                      AggregateFunction::StateCombine<StringAggState, StringAggFunction>,
	                      AggregateFunction::StateFinalize<StringAggState, string_t, StringAggFunction>,
	                      AggregateFunction::UnaryUpdate<StringAggState, string_t, StringAggFunction>, StringAggBind,
	                      AggregateFunction::StateDestroy<StringAggState, StringAggFunction>));
	string_agg.AddFunction(
	    AggregateFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, AggregateFunction::StateSize<StringAggState>,
	                      AggregateFunction::StateInitialize<StringAggState, StringAggFunction>,
	                      AggregateFunction::UnaryScatterUpdate<StringAggState, string_t, StringAggFunction>,
	                      AggregateFunction::StateCombine<StringAggState, StringAggFunction>,
	                      AggregateFunction::StateFinalize<StringAggState, string_t, StringAggFunction>,
	                      AggregateFunction::UnaryUpdate<StringAggState, string_t, StringAggFunction>, StringAggBind,
	                      AggregateFunction::StateDestroy<StringAggState, StringAggFunction>));
	set.AddFunction(string_agg);
	string_agg.name = "group_concat";
	set.AddFunction(string_agg);
}

} // namespace duckdb
