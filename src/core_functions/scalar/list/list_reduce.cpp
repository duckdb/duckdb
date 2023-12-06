#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/lambda_functions.hpp"

#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void ExecuteReduce(idx_t loops, std::vector<idx_t> &active_rows, const list_entry_t *list_entries,
                          Vector &result, Vector &left_slice, UnifiedVectorFormat &list_column_format,
                          Vector &child_vector, ClientContext &context, const Expression &lambda_expr,
                          const bool has_index, DataChunk &result_chunk, const vector<LambdaFunctions::LambdaColumnInfo> &column_infos) {
	SelectionVector right_sel(active_rows.size());
	SelectionVector left_sel(active_rows.size());

	idx_t old_count = 0;
	idx_t new_count = 0;

	auto it = active_rows.begin();
	while (it != active_rows.end()) {
		auto list_column_format_index = list_column_format.sel->get_index(*it);
		if (list_entries[list_column_format_index].length > loops + 1) {
			right_sel.set_index(new_count, list_entries[list_column_format_index].offset + loops + 1);
			left_sel.set_index(new_count, old_count);

			new_count++;
			it++;
		} else {
			if (list_entries[list_column_format_index].length == 0) {
//				throw ParameterNotAllowedException("An empty list cannot be reduced");
			} else {
				result.SetValue(*it, left_slice.GetValue(old_count));
			}
			active_rows.erase(it);
		}
		old_count++;
	}

	if (new_count == 0) {
		return;
	}

	// Execute the lambda function`
	Vector index_vector(Value::BIGINT(loops + 1));

	left_slice.Slice(left_slice, left_sel, new_count);
	Vector right_slice = Vector(child_vector, right_sel, new_count);

	unique_ptr<ExpressionExecutor> expr_executor = make_uniq<ExpressionExecutor>(context, lambda_expr);

	vector<LogicalType> input_types;
	if (has_index) {
		input_types.push_back(LogicalType::BIGINT);
	}
	input_types.push_back(right_slice.GetType());
	input_types.push_back(left_slice.GetType());
	for (auto &entry : column_infos) {
		input_types.push_back(entry.vector.get().GetType());
	}

	DataChunk input_chunk;
	input_chunk.InitializeEmpty(input_types);
	input_chunk.SetCardinality(new_count);

	idx_t slice_offset = has_index ? 1 : 0;
	if (has_index) {
		input_chunk.data[0].Reference(index_vector);
	}
	input_chunk.data[slice_offset + 1].Reference(left_slice);
	input_chunk.data[slice_offset].Reference(right_slice);

	// add the other columns
	vector<Vector> slices;
	for (idx_t i = 0; i < column_infos.size(); i++) {
		if (column_infos[i].vector.get().GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// only reference constant vectors
			input_chunk.data[slice_offset + 2 + i].Reference(column_infos[i].vector);
		} else {
			// slice the other vectors
			slices.emplace_back(column_infos[i].vector, column_infos[i].sel, new_count);
			input_chunk.data[slice_offset + 2 + i].Reference(slices.back());
		}
	}

	result_chunk.Reset();
	result_chunk.SetCardinality(new_count);

	expr_executor->Execute(input_chunk, result_chunk);

	left_slice.Reference(result_chunk.data[0]);
}

void LambdaFunctions::ListReduceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto row_count = args.size();
	Vector &list_column = args.data[0];

	// Initialize the result vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);
	if (list_column.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	if (list_column.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// get the lambda expression
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_info = func_expr.bind_info->Cast<ListLambdaBindData>();
	auto &lambda_expr = bind_info.lambda_expr;
	bool has_side_effects = lambda_expr->HasSideEffects();

	// get the list column entries
	UnifiedVectorFormat list_column_format;
	list_column.ToUnifiedFormat(row_count, list_column_format);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_column_format);

	// special-handling for the child_vector
	auto child_vector_size = ListVector::GetListSize(list_column);
	auto &child_vector = ListVector::GetEntry(list_column);
	UnifiedVectorFormat child_vector_format;
	child_vector.ToUnifiedFormat(child_vector_size, child_vector_format);

	// get the lambda column data for all other input vectors
	auto column_infos = LambdaFunctions::GetColumnInfo(args, row_count);
	auto inconstant_column_infos = LambdaFunctions::GetInconstantColumnInfo(column_infos);

	ExpressionExecutor executor(state.GetContext());

	std::vector<idx_t> active_rows(row_count);

	// Initialize the left_vector & the result vector
	SelectionVector left_vector(row_count);

	idx_t old_count = 0;
	idx_t new_count = 0;

	auto it = active_rows.begin();
	while (it != active_rows.end()) {
		auto list_column_format_index = list_column_format.sel->get_index(old_count);
		if (list_column_format.validity.RowIsValid(list_column_format_index)) {
			if (list_entries[list_column_format_index].length == 0) {
				throw ParameterNotAllowedException("An empty list cannot be reduced");
			}
			left_vector.set_index(new_count, list_entries[list_column_format_index].offset);
			active_rows[new_count] = old_count;
			new_count++;
			it++;
		} else {
			result_validity.SetInvalid(old_count);
			active_rows.erase(it);
		}
		old_count++;
	}


	Vector left_slice = Vector(child_vector, left_vector, row_count);

	DataChunk odd_result_chunk;
	odd_result_chunk.Initialize(Allocator::DefaultAllocator(), {lambda_expr->return_type});

	DataChunk even_result_chunk;
	even_result_chunk.Initialize(Allocator::DefaultAllocator(), {lambda_expr->return_type});

	idx_t loops = 0;
	// Execute reduce until all rows are finished
	while (!active_rows.empty()) {
		if (loops % 2) {
			ExecuteReduce(loops, active_rows, list_entries, result, left_slice, list_column_format, child_vector,
			              state.GetContext(), *lambda_expr, bind_info.has_index, odd_result_chunk, column_infos);
			even_result_chunk.Reset();
		} else {
			ExecuteReduce(loops, active_rows, list_entries, result, left_slice, list_column_format, child_vector,
			              state.GetContext(), *lambda_expr, bind_info.has_index, even_result_chunk, column_infos);
			odd_result_chunk.Reset();
		}

		loops++;
	}

	if (args.AllConstant() && !has_side_effects) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
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
	auto has_index = bound_lambda_expr.parameter_count == 3;

	// NULL list parameter
	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<ListLambdaBindData>(bound_function.return_type, nullptr);
	}
	// prepared statements
	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	D_ASSERT(arguments[0]->return_type.id() == LogicalTypeId::LIST);

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
	return LambdaFunctions::BindReduceLambda(parameter_idx, list_child_type);
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
