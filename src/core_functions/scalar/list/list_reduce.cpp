#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/lambda_functions.hpp"

#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ReduceColumnInfo {
	explicit ReduceColumnInfo(Vector &vector) : vector(vector), sel(SelectionVector(STANDARD_VECTOR_SIZE)) {};

	//! The original vector taken from args
	reference<Vector> vector;
	//! The selection vector to slice the original vector
	SelectionVector sel;
	//! The unified vector format of the original vector
	UnifiedVectorFormat format;
};

vector<ReduceColumnInfo> ReduceGetColumnInfo(DataChunk &args, const idx_t row_count) {

	vector<ReduceColumnInfo> data;
	// skip the input list and then insert all remaining input vectors
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		data.emplace_back(args.data[i]);
		args.data[i].ToUnifiedFormat(row_count, data.back().format);
	}
	return data;
}

vector<reference<ReduceColumnInfo>> ReduceGetInconstantColumnInfo(vector<ReduceColumnInfo> &data) {

	vector<reference<ReduceColumnInfo>> inconstant_info;
	for (auto &entry : data) {
		if (entry.vector.get().GetVectorType() != VectorType::CONSTANT_VECTOR) {
			inconstant_info.push_back(entry);
		}
	}
	return inconstant_info;
}

static void ExecuteReduce(idx_t loops, std::vector<idx_t> &active_rows, const list_entry_t *list_entries,
                          Vector &result, Vector &left_slice, Vector &child_vector, ClientContext &context,
                          const Expression &lambda_expr, const bool has_index, DataChunk &lambda_chunk,
                          const vector<ReduceColumnInfo> &column_infos) {
	SelectionVector right_sel(active_rows.size());
	SelectionVector left_sel(active_rows.size());

	idx_t old_count = 0;
	idx_t new_count = 0;

	auto it = active_rows.begin();
	while (it != active_rows.end()) {
		if (list_entries[*it].length > loops + 1) {
			right_sel.set_index(new_count, list_entries[*it].offset + loops + 1);
			left_sel.set_index(new_count, old_count);

			new_count++;
			it++;
		} else {
			result.SetValue(*it, left_slice.GetValue(old_count));
			active_rows.erase(it);
		}
		old_count++;
	}

	if (new_count == 0) {
		return;
	}

	// Execute the lambda function
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
	for (idx_t i = 0; i < column_infos.size(); i++) {
		input_chunk.data[slice_offset + 2 + i].Reference(column_infos[i].vector);
	}

	lambda_chunk.SetCardinality(new_count);

	expr_executor->Execute(input_chunk, lambda_chunk);
}

void LambdaFunctions::ListReduceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto row_count = args.size();
	Vector &list_column = args.data[0];

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

	// Initialize the result vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);
	if (list_column.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

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
			left_vector.set_index(new_count, list_entries[old_count].offset);
			active_rows[new_count] = old_count;
			new_count++;
			it++;
		} else {
			result_validity.SetInvalid(old_count);
			active_rows.erase(it);
		}
		old_count++;
	}

	// get the lambda column data for all other input vectors
	auto column_infos = ReduceGetColumnInfo(args, row_count);
	auto inconstant_column_infos = ReduceGetInconstantColumnInfo(column_infos);

	Vector left_slice = Vector(child_vector, left_vector, row_count);
	DataChunk lambda_chunk;
	lambda_chunk.Initialize(Allocator::DefaultAllocator(), {lambda_expr->return_type});

	idx_t loops = 0;
	// Execute reduce until all rows are finished
	while (!active_rows.empty()) {
		ExecuteReduce(loops, active_rows, list_entries, result, left_slice, child_vector, state.GetContext(),
		              *lambda_expr, bind_info.has_index, lambda_chunk, column_infos);

		if (active_rows.empty()) {
			break;
		}
		left_slice.Reference(lambda_chunk.data[0]);
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

	// get the lambda expression and put it in the bind info

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
