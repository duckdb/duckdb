#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

struct StringAggState {
	idx_t size;
	idx_t alloc_size;
	char *dataptr;
};

struct StringAggBindData : public FunctionData {
	explicit StringAggBindData(string sep_p) : sep(std::move(sep_p)) {
	}

	string sep;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StringAggBindData>(sep);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StringAggBindData>();
		return sep == other.sep;
	}
};

struct StringAggFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.dataptr = nullptr;
		state.alloc_size = 0;
		state.size = 0;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.dataptr) {
			finalize_data.ReturnNull();
		} else {
			target = string_t(state.dataptr, state.size);
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	static inline void PerformOperation(StringAggState &state, ArenaAllocator &allocator, const char *str,
	                                    const char *sep, idx_t str_size, idx_t sep_size) {
		if (!state.dataptr) {
			// first iteration: allocate space for the string and copy it into the state
			state.alloc_size = MaxValue<idx_t>(8, NextPowerOfTwo(str_size));
			state.dataptr = char_ptr_cast(allocator.Allocate(state.alloc_size));
			state.size = str_size;
			memcpy(state.dataptr, str, str_size);
		} else {
			// subsequent iteration: first check if we have space to place the string and separator
			idx_t required_size = state.size + str_size + sep_size;
			if (required_size > state.alloc_size) {
				// no space! allocate extra space
				const auto old_size = state.alloc_size;
				while (state.alloc_size < required_size) {
					state.alloc_size *= 2;
				}
				state.dataptr =
				    char_ptr_cast(allocator.Reallocate(data_ptr_cast(state.dataptr), old_size, state.alloc_size));
			}
			// copy the separator
			memcpy(state.dataptr + state.size, sep, sep_size);
			state.size += sep_size;
			// copy the string
			memcpy(state.dataptr + state.size, str, str_size);
			state.size += str_size;
		}
	}

	static inline void PerformOperation(StringAggState &state, ArenaAllocator &allocator, string_t str,
	                                    optional_ptr<FunctionData> data_p) {
		auto &data = data_p->Cast<StringAggBindData>();
		PerformOperation(state, allocator, str.GetData(), data.sep.c_str(), str.GetSize(), data.sep.size());
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		PerformOperation(state, unary_input.input.allocator, input, unary_input.input.bind_data);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		if (!source.dataptr) {
			// source is not set: skip combining
			return;
		}
		PerformOperation(target, aggr_input_data.allocator,
		                 string_t(source.dataptr, UnsafeNumericCast<uint32_t>(source.size)), aggr_input_data.bind_data);
	}
};

unique_ptr<FunctionData> StringAggBind(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 1) {
		// single argument: default to comma
		return make_uniq<StringAggBindData>(",");
	}
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("Separator argument to StringAgg must be a constant");
	}
	auto separator_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	string separator_string = ",";
	if (separator_val.IsNull()) {
		arguments[0] = make_uniq<BoundConstantExpression>(Value(LogicalType::VARCHAR));
	} else {
		separator_string = separator_val.ToString();
	}
	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<StringAggBindData>(std::move(separator_string));
}

static void StringAggSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const AggregateFunction &function) {
	auto bind_data = bind_data_p->Cast<StringAggBindData>();
	serializer.WriteProperty(100, "separator", bind_data.sep);
}

unique_ptr<FunctionData> StringAggDeserialize(Deserializer &deserializer, AggregateFunction &bound_function) {
	auto sep = deserializer.ReadProperty<string>(100, "separator");
	return make_uniq<StringAggBindData>(std::move(sep));
}

AggregateFunctionSet StringAggFun::GetFunctions() {
	AggregateFunctionSet string_agg;
	AggregateFunction string_agg_param(
	    {LogicalType::ANY_PARAMS(LogicalType::VARCHAR)}, LogicalType::VARCHAR,
	    AggregateFunction::StateSize<StringAggState>,
	    AggregateFunction::StateInitialize<StringAggState, StringAggFunction>,
	    AggregateFunction::UnaryScatterUpdate<StringAggState, string_t, StringAggFunction>,
	    AggregateFunction::StateCombine<StringAggState, StringAggFunction>,
	    AggregateFunction::StateFinalize<StringAggState, string_t, StringAggFunction>,
	    AggregateFunction::UnaryUpdate<StringAggState, string_t, StringAggFunction>, StringAggBind);
	string_agg_param.serialize = StringAggSerialize;
	string_agg_param.deserialize = StringAggDeserialize;
	string_agg.AddFunction(string_agg_param);
	string_agg_param.arguments.emplace_back(LogicalType::VARCHAR);
	string_agg.AddFunction(string_agg_param);
	return string_agg;
}

} // namespace duckdb
