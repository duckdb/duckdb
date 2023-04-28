#include "duckdb/function/table/range.hpp"

#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/projection/physical_range.hpp"
#include "duckdb/execution/operator/projection/physical_time_range.hpp"


namespace duckdb {

//===--------------------------------------------------------------------===//
// Range (integers) and Range (timestamps)
//===--------------------------------------------------------------------===//

struct RangeFunctionBindData : public FunctionData {
	explicit RangeFunctionBindData(const idx_t &num_given_args_p, const vector<LogicalType> &input_types_p, 
								   bool with_timestamps_p) : 
		num_given_args(num_given_args_p), input_types(std::move(input_types_p)), with_timestamps(with_timestamps_p) {

			D_ASSERT(num_given_args == input_types.size());
	}

	const idx_t num_given_args;
	const vector<LogicalType> input_types;
	const bool with_timestamps; // true: range (timestamps), false: range (integers)

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RangeFunctionBindData>(num_given_args, input_types, with_timestamps);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const RangeFunctionBindData &)other_p;
		if (with_timestamps != other.with_timestamps) {
			return false;
		}
		if (num_given_args != other.num_given_args) {
			return false;
		}
		if (input_types.size() != other.input_types.size()) {
			return false;
		}
		for (idx_t i = 0; i < input_types.size(); i++) {
			if (input_types[i] != other.input_types[i]) {
				return false;
			}
		}
		return true;
	}
};


static unique_ptr<FunctionData> RangeFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {

	LogicalType &first_arg_type = input.input_table_types[0];
	idx_t num_given_args = input.input_table_types.size();

	names.push_back("range");

	bool with_timestamps; 

	// check whether first argument can be implicitly cast to bigint
	if (CastFunctionSet::Get(context).ImplicitCastCost(first_arg_type, LogicalType::BIGINT) >= 0) {
		// Range (integers)
		with_timestamps = false;
		return_types.push_back(LogicalType::BIGINT);
		if (num_given_args < 1 || num_given_args > 3) {
			throw BinderException("GENERATE_SERIES / RANGE requires between 1 and 3 arguments");
		}
		for (idx_t i = 1; i < num_given_args; i++) {
			if (CastFunctionSet::Get(context).ImplicitCastCost(input.input_table_types[i], LogicalType::BIGINT) < 0) {
				throw BinderException("GENERATE_SERIES / RANGE requires arguments of type BIGINT");
			}
		}
	} else if (CastFunctionSet::Get(context).ImplicitCastCost(first_arg_type, LogicalType::TIMESTAMP) >= 0) {
		// Range (timestamps)
		with_timestamps = true;
		return_types.push_back(LogicalType::TIMESTAMP);
		if (num_given_args != 3) {
			throw BinderException("GENERATE_SERIES / RANGE (timestamps) requires 3 arguments");
		}
		if (CastFunctionSet::Get(context).ImplicitCastCost(input.input_table_types[1], LogicalType::TIMESTAMP) < 0 ||
		    CastFunctionSet::Get(context).ImplicitCastCost(input.input_table_types[2], LogicalType::INTERVAL) < 0) {

			throw BinderException(
				"GENERATE_SERIES / RANGE (timestamps) requires 3 arguments of types (TIMESTAMP, TIMESTAMP, INTERVAL)");
		}
	} else {
		throw BinderException("First argument of GENERATE_SERIES / RANGE must be either of type BIGINT or TIMESTAMP");
	}
	return make_uniq<RangeFunctionBindData>(num_given_args, input.input_table_types, with_timestamps);
}

struct RangeFunctionGlobalState : public GlobalTableFunctionState {
	RangeFunctionGlobalState() {
	}

	// a list of bound expressions representing the function's arguments
	vector<unique_ptr<Expression>> args_list;

	idx_t MaxThreads() const override {
		return GlobalTableFunctionState::MAX_THREADS;
	}
};

struct RangeFunctionLocalState : public LocalTableFunctionState {
	RangeFunctionLocalState() {
	}

	unique_ptr<OperatorState> operator_state;
};

template <bool GENERATE_SERIES>
static unique_ptr<LocalTableFunctionState> RangeFunctionLocalInit(ExecutionContext &context, 
                                                                  TableFunctionInitInput &input,
                                                           		  GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<RangeFunctionGlobalState>();
	auto &bind_data = input.bind_data->Cast<RangeFunctionBindData>();

	// initialize operator state according to whether the function is applied to integers or timestamps
	auto result = make_uniq<RangeFunctionLocalState>();
	if (bind_data.with_timestamps) {
		result->operator_state = PhysicalTimeRange::GetState(context, gstate.args_list, GENERATE_SERIES);
	} else {
		result->operator_state = PhysicalRange::GetState(context, gstate.args_list, GENERATE_SERIES);
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RangeFunctionInit(ClientContext &context, TableFunctionInitInput &input) {

	auto &bind_data = input.bind_data->Cast<RangeFunctionBindData>();
	auto result = make_uniq<RangeFunctionGlobalState>();
	
	// initialize the global state's args_list with bound expressions referencing the input columns
	for(idx_t i = 0; i < bind_data.num_given_args; i++) {
		auto expr = make_uniq<BoundReferenceExpression>(bind_data.input_types[i], i);
		result->args_list.push_back(std::move(expr));
	}	

	return std::move(result);
}

static OperatorResultType RangeFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                        DataChunk &output) {
	auto &state = data_p.global_state->Cast<RangeFunctionGlobalState>();
	auto &lstate = data_p.local_state->Cast<RangeFunctionLocalState>();
	auto &bind_data = data_p.bind_data->Cast<RangeFunctionBindData>();
	if (bind_data.with_timestamps) {
		return PhysicalTimeRange::ExecuteInternal(context, input, output, *lstate.operator_state, state.args_list);
	} else {
		return PhysicalRange::ExecuteInternal(context, input, output, *lstate.operator_state, state.args_list);
	}
}


void RangeTableFunction::RegisterFunction(BuiltinFunctions &set) {
	
	TableFunction range_function("range", {LogicalTypeId::TABLE}, nullptr, 
	                                       RangeFunctionBind, RangeFunctionInit, RangeFunctionLocalInit<false>);
	range_function.in_out_function = RangeFunction;
	set.AddFunction(range_function);

	// generate_series: similar to range, but inclusive instead of exclusive bounds on the RHS
	TableFunction generate_series_function("generate_series", {LogicalTypeId::TABLE}, nullptr, 
	                                       RangeFunctionBind, RangeFunctionInit, RangeFunctionLocalInit<true>);
	generate_series_function.in_out_function = RangeFunction;
	set.AddFunction(generate_series_function);
}

void BuiltinFunctions::RegisterTableFunctions() {
	CheckpointFunction::RegisterFunction(*this);
	GlobTableFunction::RegisterFunction(*this);
	RangeTableFunction::RegisterFunction(*this);
	RepeatTableFunction::RegisterFunction(*this);
	SummaryTableFunction::RegisterFunction(*this);
	UnnestTableFunction::RegisterFunction(*this);
	RepeatRowTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
