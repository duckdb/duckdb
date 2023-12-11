#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ReduceExecuteInfo {
	ReduceExecuteInfo(LambdaFunctions::LambdaInfo &info, ClientContext &context) : left_slice(*info.child_vector) {
		SelectionVector left_vector(info.row_count);
		active_rows.resize(info.row_count);

		idx_t old_count = 0;
		idx_t new_count = 0;

		// Initialize the left vector from list entries and the active rows
		auto it = active_rows.begin();
		while (it != active_rows.end()) {
			auto list_column_format_index = info.list_column_format.sel->get_index(old_count);
			if (info.list_column_format.validity.RowIsValid(list_column_format_index)) {
				if (info.list_entries[list_column_format_index].length == 0) {
					throw ParameterNotAllowedException("Cannot perform list_reduce on an empty input list");
				}
				left_vector.set_index(new_count, info.list_entries[list_column_format_index].offset);
				active_rows[new_count] = old_count;
				new_count++;
				it++;
			} else {
				// Remove the invalid rows
				info.result_validity->SetInvalid(old_count);
				active_rows.erase(it);
			}
			old_count++;
		}

		left_slice.Slice(left_vector, new_count);

		if (info.has_index) {
			input_types.push_back(LogicalType::BIGINT);
		}
		input_types.push_back(left_slice.GetType());
		input_types.push_back(left_slice.GetType());
		for (auto &entry : info.column_infos) {
			input_types.push_back(entry.vector.get().GetType());
		}

		expr_executor = make_uniq<ExpressionExecutor>(context, *info.lambda_expr);
	};

	vector<idx_t> active_rows;
	Vector left_slice;
	unique_ptr<ExpressionExecutor> expr_executor;
	vector<LogicalType> input_types;
};

static void ExecuteReduce(idx_t loops, ReduceExecuteInfo &execute_info, LambdaFunctions::LambdaInfo &info,
                          DataChunk &result_chunk) {
	SelectionVector right_sel(execute_info.active_rows.size());
	SelectionVector left_sel(execute_info.active_rows.size());
	SelectionVector active_rows_sel(execute_info.active_rows.size());

	idx_t old_count = 0;
	idx_t new_count = 0;

	// create selection vectors for the left and right slice
	auto it = execute_info.active_rows.begin();
	while (it != execute_info.active_rows.end()) {
		auto list_column_format_index = info.list_column_format.sel->get_index(*it);
		if (info.list_entries[list_column_format_index].length > loops + 1) {
			right_sel.set_index(new_count, info.list_entries[list_column_format_index].offset + loops + 1);
			left_sel.set_index(new_count, old_count);
			active_rows_sel.set_index(new_count, *it);

			new_count++;
			it++;
		} else {
			info.result.SetValue(*it, execute_info.left_slice.GetValue(old_count));
			execute_info.active_rows.erase(it);
		}
		old_count++;
	}

	if (new_count == 0) {
		return;
	}

	// create the index vector
	Vector index_vector(Value::BIGINT(loops + 1));

	// slice the left and right slice
	execute_info.left_slice.Slice(execute_info.left_slice, left_sel, new_count);
	Vector right_slice(*info.child_vector, right_sel, new_count);

	// create the input chunk
	DataChunk input_chunk;
	input_chunk.InitializeEmpty(execute_info.input_types);
	input_chunk.SetCardinality(new_count);

	idx_t slice_offset = info.has_index ? 1 : 0;
	if (info.has_index) {
		input_chunk.data[0].Reference(index_vector);
	}
	input_chunk.data[slice_offset + 1].Reference(execute_info.left_slice);
	input_chunk.data[slice_offset].Reference(right_slice);

	// add the other columns
	vector<Vector> slices;
	for (idx_t i = 0; i < info.column_infos.size(); i++) {
		if (info.column_infos[i].vector.get().GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// only reference constant vectors
			input_chunk.data[slice_offset + 2 + i].Reference(info.column_infos[i].vector);
		} else {
			// slice the other vectors
			slices.emplace_back(info.column_infos[i].vector, active_rows_sel, new_count);
			input_chunk.data[slice_offset + 2 + i].Reference(slices.back());
		}
	}

	result_chunk.Reset();
	result_chunk.SetCardinality(new_count);

	execute_info.expr_executor->Execute(input_chunk, result_chunk);

	// use the result chunk to update the left slice
	execute_info.left_slice.Reference(result_chunk.data[0]);
}

void LambdaFunctions::ListReduceFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state,
                                         duckdb::Vector &result) {
	// Initializes the left slice from the list entries, active rows, the expression executor and the input types
	bool completed = false;
	LambdaFunctions::LambdaInfo info(args, state, result, completed);
	if (completed) {
		return;
	}

	ReduceExecuteInfo execute_info(info, state.GetContext());

	// Since the left slice references the result chunk, we need to create two result chunks.
	// This means there is always an empty result chunk for the next iteration,
	// without the referenced chunk having to be reset until the current iteration is complete.
	DataChunk odd_result_chunk;
	odd_result_chunk.Initialize(Allocator::DefaultAllocator(), {info.lambda_expr->return_type});

	DataChunk even_result_chunk;
	even_result_chunk.Initialize(Allocator::DefaultAllocator(), {info.lambda_expr->return_type});

	idx_t loops = 0;
	// Execute reduce until all rows are finished
	while (!execute_info.active_rows.empty()) {
		if (loops % 2) {
			ExecuteReduce(loops, execute_info, info, odd_result_chunk);
			even_result_chunk.Reset();
		} else {
			ExecuteReduce(loops, execute_info, info, even_result_chunk);
			odd_result_chunk.Reset();
		}

		loops++;
	}

	if (info.is_all_constant && !info.has_side_effects) {
		info.result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListReduceBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	// the list column and the bound lambda expression
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->expression_class != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.parameter_count < 2 || bound_lambda_expr.parameter_count > 3) {
		throw BinderException("list_reduce expects a function with 2 or 3 arguments");
	}
	auto has_index = bound_lambda_expr.parameter_count == 3;

	unique_ptr<FunctionData> bind_data = LambdaFunctions::ListLambdaPrepareBind(arguments, context, bound_function);
	if (bind_data) {
		return bind_data;
	}

	auto list_child_type = arguments[0]->return_type;
	list_child_type = ListType::GetChildType(list_child_type);

	auto cast_lambda_expr =
	    BoundCastExpression::AddCastToType(context, std::move(bound_lambda_expr.lambda_expr), list_child_type, false);
	if (!cast_lambda_expr) {
		throw BinderException("Could not cast lambda expression to list child type");
	}
	bound_function.return_type = cast_lambda_expr->return_type;
	return make_uniq<ListLambdaBindData>(bound_function.return_type, std::move(cast_lambda_expr), has_index);
}

static LogicalType ListReduceBindLambda(const idx_t parameter_idx, const LogicalType &list_child_type) {
	return LambdaFunctions::BindTertiaryLambda(parameter_idx, list_child_type);
}

ScalarFunction ListReduceFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::ANY,
	                   LambdaFunctions::ListReduceFunction, ListReduceBind, nullptr, nullptr);

	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	fun.bind_lambda = ListReduceBindLambda;

	return fun;
}

} // namespace duckdb
