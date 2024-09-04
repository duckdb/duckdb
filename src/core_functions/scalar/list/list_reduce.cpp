#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ReduceExecuteInfo {
	ReduceExecuteInfo(LambdaFunctions::LambdaInfo &info, ClientContext &context)
	    : left_slice(make_uniq<Vector>(*info.child_vector)) {
		SelectionVector left_vector(info.row_count);
		active_rows.Resize(0, info.row_count);
		active_rows.SetAllValid(info.row_count);

		left_sel.Initialize(info.row_count);
		active_rows_sel.Initialize(info.row_count);

		idx_t reduced_row_idx = 0;

		for (idx_t original_row_idx = 0; original_row_idx < info.row_count; original_row_idx++) {
			auto list_column_format_index = info.list_column_format.sel->get_index(original_row_idx);
			if (info.list_column_format.validity.RowIsValid(list_column_format_index)) {
				if (info.list_entries[list_column_format_index].length == 0) {
					throw ParameterNotAllowedException("Cannot perform list_reduce on an empty input list");
				}
				left_vector.set_index(reduced_row_idx, info.list_entries[list_column_format_index].offset);
				reduced_row_idx++;
			} else {
				// Set the row as invalid and remove it from the active rows.
				FlatVector::SetNull(info.result, original_row_idx, true);
				active_rows.SetInvalid(original_row_idx);
			}
		}
		left_slice->Slice(left_vector, reduced_row_idx);

		if (info.has_index) {
			input_types.push_back(LogicalType::BIGINT);
		}
		input_types.push_back(left_slice->GetType());
		input_types.push_back(left_slice->GetType());
		for (auto &entry : info.column_infos) {
			input_types.push_back(entry.vector.get().GetType());
		}

		expr_executor = make_uniq<ExpressionExecutor>(context, *info.lambda_expr);
	};
	ValidityMask active_rows;
	unique_ptr<Vector> left_slice;
	unique_ptr<ExpressionExecutor> expr_executor;
	vector<LogicalType> input_types;

	SelectionVector left_sel;
	SelectionVector active_rows_sel;
};

static bool ExecuteReduce(idx_t loops, ReduceExecuteInfo &execute_info, LambdaFunctions::LambdaInfo &info,
                          DataChunk &result_chunk) {
	idx_t original_row_idx = 0;
	idx_t reduced_row_idx = 0;
	idx_t valid_row_idx = 0;

	// create selection vectors for the left and right slice
	auto data = execute_info.active_rows.GetData();

	// reset right_sel each iteration to prevent referencing issues
	SelectionVector right_sel;
	right_sel.Initialize(info.row_count);

	idx_t bits_per_entry = sizeof(idx_t) * 8;
	for (idx_t entry_idx = 0; original_row_idx < info.row_count; entry_idx++) {
		if (data[entry_idx] == 0) {
			original_row_idx += bits_per_entry;
			continue;
		}

		for (idx_t j = 0; entry_idx * bits_per_entry + j < info.row_count; j++) {
			if (!execute_info.active_rows.RowIsValid(original_row_idx)) {
				original_row_idx++;
				continue;
			}
			auto list_column_format_index = info.list_column_format.sel->get_index(original_row_idx);
			if (info.list_entries[list_column_format_index].length > loops + 1) {
				right_sel.set_index(reduced_row_idx, info.list_entries[list_column_format_index].offset + loops + 1);
				execute_info.left_sel.set_index(reduced_row_idx, valid_row_idx);
				execute_info.active_rows_sel.set_index(reduced_row_idx, original_row_idx);
				reduced_row_idx++;

			} else {
				execute_info.active_rows.SetInvalid(original_row_idx);
				auto val = execute_info.left_slice->GetValue(valid_row_idx);
				info.result.SetValue(original_row_idx, val);
			}

			original_row_idx++;
			valid_row_idx++;
		}
	}

	if (reduced_row_idx == 0) {
		return true;
	}

	// create the index vector
	Vector index_vector(Value::BIGINT(UnsafeNumericCast<int64_t>(loops + 1)));

	// slice the left and right slice
	execute_info.left_slice->Slice(*execute_info.left_slice, execute_info.left_sel, reduced_row_idx);
	Vector right_slice(*info.child_vector, right_sel, reduced_row_idx);

	// create the input chunk
	DataChunk input_chunk;
	input_chunk.InitializeEmpty(execute_info.input_types);
	input_chunk.SetCardinality(reduced_row_idx);

	idx_t slice_offset = info.has_index ? 1 : 0;
	if (info.has_index) {
		input_chunk.data[0].Reference(index_vector);
	}
	input_chunk.data[slice_offset + 1].Reference(*execute_info.left_slice);
	input_chunk.data[slice_offset].Reference(right_slice);

	// add the other columns
	vector<Vector> slices;
	for (idx_t i = 0; i < info.column_infos.size(); i++) {
		if (info.column_infos[i].vector.get().GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// only reference constant vectors
			input_chunk.data[slice_offset + 2 + i].Reference(info.column_infos[i].vector);
		} else {
			// slice the other vectors
			slices.emplace_back(info.column_infos[i].vector, execute_info.active_rows_sel, reduced_row_idx);
			input_chunk.data[slice_offset + 2 + i].Reference(slices.back());
		}
	}

	result_chunk.Reset();
	result_chunk.SetCardinality(reduced_row_idx);
	execute_info.expr_executor->Execute(input_chunk, result_chunk);

	// We need to copy the result into left_slice to avoid data loss due to vector.Reference(...).
	// Otherwise, we only keep the data of the previous iteration alive, not that of previous iterations.
	execute_info.left_slice = make_uniq<Vector>(result_chunk.data[0].GetType(), reduced_row_idx);
	VectorOperations::Copy(result_chunk.data[0], *execute_info.left_slice, reduced_row_idx, 0, 0);
	return false;
}

void LambdaFunctions::ListReduceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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

	// Execute reduce until all rows are finished.
	idx_t loops = 0;
	bool end = false;
	while (!end) {
		auto &result_chunk = loops % 2 ? odd_result_chunk : even_result_chunk;
		auto &spare_result_chunk = loops % 2 ? even_result_chunk : odd_result_chunk;

		end = ExecuteReduce(loops, execute_info, info, result_chunk);
		spare_result_chunk.Reset();
		loops++;
	}

	if (info.is_all_constant && !info.is_volatile) {
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
	return LambdaFunctions::BindTernaryLambda(parameter_idx, list_child_type);
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
