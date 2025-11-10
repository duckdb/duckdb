#include "core_functions/scalar/list_functions.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

namespace {

struct ReduceExecuteInfo {
	ReduceExecuteInfo(LambdaFunctions::LambdaInfo &info, ClientContext &context)
	    : left_slice(make_uniq<Vector>(*info.child_vector)) {
		if (info.has_initial) {
			initial_value_offset = 0;
		}
		SelectionVector left_vector(info.row_count);
		active_rows.Resize(info.row_count);
		active_rows.SetAllValid(info.row_count);

		left_sel.Initialize(info.row_count);
		active_rows_sel.Initialize(info.row_count);

		idx_t reduced_row_idx = 0;

		if (info.has_initial) {
			left_vector.set_index(0, 0);
		}

		for (idx_t original_row_idx = 0; original_row_idx < info.row_count; original_row_idx++) {
			auto list_column_format_index = info.list_column_format.sel->get_index(original_row_idx);
			if (info.list_column_format.validity.RowIsValid(list_column_format_index)) {
				if (info.list_entries[list_column_format_index].length == 0 && !info.has_initial) {
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
		// skip the first entry if there is an initial value
		for (idx_t i = info.has_initial ? 1 : 0; i < info.column_infos.size(); i++) {
			input_types.push_back(info.column_infos[i].vector.get().GetType());
		}

		expr_executor = make_uniq<ExpressionExecutor>(context, *info.lambda_expr);
	};

	ValidityMask active_rows;
	unique_ptr<Vector> left_slice;
	unique_ptr<ExpressionExecutor> expr_executor;
	vector<LogicalType> input_types;

	idx_t initial_value_offset = 1;
	SelectionVector left_sel;
	SelectionVector active_rows_sel;
};

bool ExecuteReduce(const idx_t loops, ReduceExecuteInfo &execute_info, LambdaFunctions::LambdaInfo &info,
                   DataChunk &result_chunk) {
	idx_t original_row_idx = 0;
	idx_t reduced_row_idx = 0;
	idx_t valid_row_idx = 0;

	idx_t loops_offset = loops + execute_info.initial_value_offset;

	// create selection vectors for the left and right slice
	const auto data = execute_info.active_rows.GetData();

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
			if (info.list_entries[list_column_format_index].length > loops_offset) {
				// While the list has more entries set the right slice to the next entry
				right_sel.set_index(reduced_row_idx, info.list_entries[list_column_format_index].offset + loops_offset);
				execute_info.left_sel.set_index(reduced_row_idx, valid_row_idx);
				execute_info.active_rows_sel.set_index(reduced_row_idx, original_row_idx);
				reduced_row_idx++;

			} else if (info.list_entries[list_column_format_index].length == 0 && info.has_initial &&
			           loops_offset == 0) {
				// If the list is empty and there is an initial value, use the initial value
				execute_info.active_rows.SetInvalid(original_row_idx);
				auto val = info.column_infos[0].vector.get().GetValue(original_row_idx);
				info.result.SetValue(original_row_idx, val);
			} else {
				// If the list has no more entries, write the result
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

	// create the index vector, where the index is that of the current node.
	Vector index_vector(Value::BIGINT(UnsafeNumericCast<int64_t>(loops_offset + 1)));

	// slice the left and right slice
	execute_info.left_slice->Slice(*execute_info.left_slice, execute_info.left_sel, reduced_row_idx);
	Vector right_slice(*info.child_vector, right_sel, reduced_row_idx);

	// create the input chunk
	DataChunk input_chunk;
	input_chunk.InitializeEmpty(execute_info.input_types);
	input_chunk.SetCardinality(reduced_row_idx);

	const idx_t slice_offset = info.has_index ? 1 : 0;
	if (info.has_index) {
		input_chunk.data[0].Reference(index_vector);
	}

	if (loops == 0 && info.has_initial) {
		info.column_infos[0].vector.get().Slice(execute_info.active_rows_sel, reduced_row_idx);
		input_chunk.data[slice_offset + 1].Reference(info.column_infos[0].vector);
	} else {
		input_chunk.data[slice_offset + 1].Reference(*execute_info.left_slice);
	}
	input_chunk.data[slice_offset].Reference(right_slice);

	// add the other columns
	// skip the initial value if there is one
	vector<Vector> slices;
	const idx_t initial_offset = info.has_initial ? 1 : 0;
	for (idx_t i = 0; i < info.column_infos.size() - initial_offset; i++) {
		if (info.column_infos[i].vector.get().GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// only reference constant vectors
			input_chunk.data[slice_offset + 2 + i].Reference(info.column_infos[initial_offset + i].vector);
		} else {
			// slice the other vectors
			slices.emplace_back(info.column_infos[initial_offset + i].vector, execute_info.active_rows_sel,
			                    reduced_row_idx);
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

unique_ptr<FunctionData> ListReduceBind(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments) {
	// the list column and the bound lambda expression
	D_ASSERT(arguments.size() == 2 || arguments.size() == 3);
	if (arguments[1]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	unique_ptr<FunctionData> bind_data = LambdaFunctions::ListLambdaPrepareBind(arguments, context, bound_function);
	if (bind_data) {
		return bind_data;
	}

	auto list_child_type = arguments[0]->return_type;
	list_child_type = ListType::GetChildType(list_child_type);

	bool has_initial = arguments.size() == 3;
	if (has_initial) {
		const auto initial_value_type = arguments[2]->return_type;
		// Check if the initial value type is the same as the return type of the lambda expression
		if (list_child_type != initial_value_type) {
			LogicalType max_logical_type;
			const auto has_max_logical_type =
			    LogicalType::TryGetMaxLogicalType(context, list_child_type, initial_value_type, max_logical_type);
			if (!has_max_logical_type) {
				throw BinderException(
				    "The initial value type must be the same as the list child type or a common super type");
			}

			list_child_type = max_logical_type;
			arguments[0] = BoundCastExpression::AddCastToType(context, std::move(arguments[0]),
			                                                  LogicalType::LIST(max_logical_type));
			arguments[2] = BoundCastExpression::AddCastToType(context, std::move(arguments[2]), max_logical_type);
		}
	}

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.parameter_count < 2 || bound_lambda_expr.parameter_count > 3) {
		throw BinderException("list_reduce expects a function with 2 or 3 arguments");
	}
	auto has_index = bound_lambda_expr.parameter_count == 3;

	auto cast_lambda_expr =
	    BoundCastExpression::AddCastToType(context, std::move(bound_lambda_expr.lambda_expr), list_child_type);
	if (!cast_lambda_expr) {
		throw BinderException("Could not cast lambda expression to list child type");
	}
	bound_function.SetReturnType(cast_lambda_expr->return_type);
	return make_uniq<ListLambdaBindData>(bound_function.GetReturnType(), std::move(cast_lambda_expr), has_index,
	                                     has_initial);
}

LogicalType BindReduceChildren(ClientContext &context, const vector<LogicalType> &function_child_types,
                               const idx_t parameter_idx) {
	auto list_child_type = LambdaFunctions::DetermineListChildType(function_child_types[0]);

	// if there is an initial value, find the max logical type
	if (function_child_types.size() == 3) {
		// the initial value is the third child
		constexpr idx_t initial_idx = 2;

		const LogicalType initial_value_type = function_child_types[initial_idx];
		if (initial_value_type != list_child_type) {
			// we need to check if the initial value type is the same as the return type of the lambda expression
			LogicalType max_logical_type;
			const auto has_max_logical_type =
			    LogicalType::TryGetMaxLogicalType(context, list_child_type, initial_value_type, max_logical_type);
			if (!has_max_logical_type) {
				throw BinderException(
				    "The initial value type must be the same as the list child type or a common super type");
			}

			list_child_type = max_logical_type;
		}
	}

	switch (parameter_idx) {
	case 0:
		return list_child_type;
	case 1:
		return list_child_type;
	case 2:
		return LogicalType::BIGINT;
	default:
		throw BinderException("This lambda function only supports up to three lambda parameters!");
	}
}

LogicalType ListReduceBindLambda(ClientContext &context, const vector<LogicalType> &function_child_types,
                                 const idx_t parameter_idx) {
	return BindReduceChildren(context, function_child_types, parameter_idx);
}

} // namespace

void LambdaFunctions::ListReduceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Initializes the left slice from the list entries, active rows, the expression executor and the input types
	bool completed = false;
	LambdaInfo info(args, state, result, completed);
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

ScalarFunctionSet ListReduceFun::GetFunctions() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::ANY,
	                   LambdaFunctions::ListReduceFunction, ListReduceBind, nullptr, nullptr);

	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	fun.bind_lambda = ListReduceBindLambda;

	ScalarFunctionSet set;
	set.AddFunction(fun);
	fun.arguments.push_back(LogicalType::ANY);
	set.AddFunction(fun);
	return set;
}

} // namespace duckdb
