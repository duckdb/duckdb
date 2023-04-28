#include "duckdb/execution/operator/projection/physical_range.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Range (timestamp)
//===--------------------------------------------------------------------===//

class RangeOperatorState : public OperatorState {
public:
	RangeOperatorState(ClientContext &context, const vector<unique_ptr<Expression>> &args_list, bool generate_series_p)
	    : current_row(0), first_fetch(true), generate_series(generate_series_p), executor(context) {

		// we add each expression in the args_list to the expression executor
		vector<LogicalType> args_data_types;
		for (auto &exp : args_list) {
			D_ASSERT(exp->type == ExpressionType::BOUND_REF);
			auto &ref = exp->Cast<BoundReferenceExpression>();
			args_data_types.push_back(exp->return_type);
			executor.AddExpression(ref);
		}

		auto &allocator = Allocator::Get(context);
		args_data.Initialize(allocator, args_data_types);
	}

	//! loads arguments from args_list, using the data in the input chunk
	void FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list);

	//! number of already generated rows
	idx_t current_row;
	bool first_fetch;

	//! indicate whether the function is generate_series (true) or range (false)
	bool generate_series;

	const hugeint_t DEFAULT_INCREMENT = 1;
	const hugeint_t DEFAULT_START = 0;
	hugeint_t start;
	hugeint_t end;
	hugeint_t increment;

	ExpressionExecutor executor;
	//! chunk for the arguments to be stored in
	DataChunk args_data;

public:
	//! Reset the fields of the range operator state
	void Reset();
};

void RangeOperatorState::Reset() {
	current_row = 0;
	first_fetch = true;
}

void RangeOperatorState::FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list) {
	
	args_data.Reset();
	// execute the argument expressions
	// execution results (arguments) are kept in args_data chunk
	executor.Execute(input, args_data);

	// verify incoming arguments
	args_data.Verify();
	D_ASSERT(input.size() == args_data.size());
	D_ASSERT(args_data.ColumnCount() == args_list.size());
	D_ASSERT(args_list.size() <= 3 && args_list.size() >= 1);

	first_fetch = false;

	if(args_list.size() < 3) {
		// no increment is given, set it to default value
		increment = DEFAULT_INCREMENT;
	} else {
		increment = args_data.GetValue(2,0).GetValue<int64_t>();
	}
	if(args_list.size() == 1) {
		// if only one argument is given, use it as end and set start to default value
		start = DEFAULT_START;
		end = args_data.GetValue(0,0).GetValue<int64_t>();
	} else {
		start = args_data.GetValue(0,0).GetValue<int64_t>();
		end = args_data.GetValue(1,0).GetValue<int64_t>();
	}
	if(generate_series) { // inclusive upper bound for generate_series
		if (increment < 0) {
			end = end - 1;
		} else {
			end = end + 1;
		}
	}

	if (increment == 0) {
		throw InvalidInputException("increment cannot be 0!");
	}
	if (start > end && increment > 0) {
		throw InvalidInputException(
			"start is bigger than end, but increment is positive: cannot generate infinite series");
	} else if (start < end && increment < 0) {
		throw InvalidInputException(
			"start is smaller than end, but increment is negative: cannot generate infinite series");
	}
}

unique_ptr<OperatorState> PhysicalRange::GetOperatorState(ExecutionContext &context) const {
	return PhysicalRange::GetState(context, args_list, generate_series);
}

unique_ptr<OperatorState> PhysicalRange::GetState(ExecutionContext &context,
                                                   const vector<unique_ptr<Expression>> &args_list,
												   bool generate_series) {
	return make_uniq<RangeOperatorState>(context.client, args_list, generate_series);
}

OperatorResultType PhysicalRange::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   OperatorState &state_p,
                                                   const vector<unique_ptr<Expression>> &args_list) {

	auto &state = state_p.Cast<RangeOperatorState>();

	if(state.first_fetch) {
		state.FetchArguments(input, args_list);
	}
	
	hugeint_t current_value = state.start + state.increment * state.current_row;
	int64_t current_value_i64 = Hugeint::Cast<int64_t>(current_value);
	int64_t offset = state.increment < 0 ? 1 : -1;
	
	// number of remaining rows to generate
	idx_t remaining = Hugeint::Cast<idx_t>((state.end - current_value + (state.increment + offset)) / state.increment);
	 // limit # of generated rows to STANDARD_VECTOR_SIZE
	idx_t num_new_rows = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);

	int64_t increment_i64 = Hugeint::Cast<int64_t>(state.increment);
	chunk.data[0].Sequence(current_value_i64, increment_i64, num_new_rows);
	chunk.SetCardinality(num_new_rows);
	chunk.Verify();

	if(remaining > num_new_rows) {
		// there are still rows to be generated 
		state.current_row += num_new_rows;
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		// finished
		state.Reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

OperatorResultType PhysicalRange::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &, OperatorState &state) const {
	return ExecuteInternal(context, input, chunk, state, args_list);
}

} // namespace duckdb
