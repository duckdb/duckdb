#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

struct ListLambdaBindData : public FunctionData {
	ListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr);
	~ListLambdaBindData() override;

	LogicalType stype;
	unique_ptr<Expression> lambda_expr;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ListLambdaBindData::ListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr_p)
    : stype(stype_p), lambda_expr(move(lambda_expr_p)) {
}

unique_ptr<FunctionData> ListLambdaBindData::Copy() const {
	return make_unique<ListLambdaBindData>(stype, lambda_expr->Copy());
}

bool ListLambdaBindData::Equals(const FunctionData &other_p) const {
	auto &other = (ListLambdaBindData &)other_p;
	return lambda_expr->Equals(other.lambda_expr.get()) && stype == other.stype;
}

ListLambdaBindData::~ListLambdaBindData() {
}

static void AppendTransformedToResult(Vector &lambda_vector, idx_t &elem_cnt, Vector &result) {

	// append the lambda_vector to the result list
	VectorData lambda_child_data;
	lambda_vector.Orrify(elem_cnt, lambda_child_data);
	ListVector::Append(result, lambda_vector, *lambda_child_data.sel, elem_cnt, 0);
}

static void AppendFilteredToResult(Vector &lambda_vector, list_entry_t *result_entries, idx_t &elem_cnt, Vector &result,
                                   idx_t &curr_list_len, idx_t &curr_list_offset, idx_t &appended_lists_cnt,
                                   vector<idx_t> &lists_len, idx_t &curr_original_list_len, DataChunk &input_chunk) {

	idx_t true_count = 0;
	SelectionVector true_sel(STANDARD_VECTOR_SIZE);
	auto lambda_values = FlatVector::GetData<bool>(lambda_vector);
	auto &lambda_validity = FlatVector::Validity(lambda_vector);

	while (lists_len[appended_lists_cnt] == 0) {
		result_entries[appended_lists_cnt].offset = curr_list_offset;
		result_entries[appended_lists_cnt].length = 0;
		appended_lists_cnt++;
		if (appended_lists_cnt == lists_len.size()) {
			break;
		}
	}

	// compute the new lengths and offsets, and create a selection vector
	for (idx_t i = 0; i < elem_cnt; i++) {

		while (lists_len[appended_lists_cnt] == 0) {
			result_entries[appended_lists_cnt].offset = curr_list_offset;
			result_entries[appended_lists_cnt].length = 0;
			appended_lists_cnt++;
			if (appended_lists_cnt == lists_len.size()) {
				break;
			}
		}

		// found a true value
		if (lambda_validity.RowIsValid(i)) {
			if (lambda_values[i] > 0) {
				true_sel.set_index(true_count++, i);
				curr_list_len++;
			}
		}
		curr_original_list_len++;

		if (lists_len[appended_lists_cnt] == curr_original_list_len) {
			result_entries[appended_lists_cnt].offset = curr_list_offset;
			result_entries[appended_lists_cnt].length = curr_list_len;
			curr_list_offset += curr_list_len;
			appended_lists_cnt++;
			curr_list_len = 0;
			curr_original_list_len = 0;
		}
	}

	// slice to get the new lists and append them to the result
	Vector new_lists(input_chunk.data[0], true_sel, true_count);
	new_lists.Normalify(true_count);
	VectorData new_lists_child_data;
	new_lists.Orrify(true_count, new_lists_child_data);
	ListVector::Append(result, new_lists, *new_lists_child_data.sel, true_count, 0);
}

static void ExecuteExpression(vector<LogicalType> &types, vector<LogicalType> &result_types, idx_t &elem_cnt,
                              SelectionVector &sel, vector<SelectionVector> &sel_vectors, DataChunk &input_chunk,
                              DataChunk &lambda_chunk, Vector &child_vector, DataChunk &args,
                              ExpressionExecutor &expr_executor) {

	input_chunk.SetCardinality(elem_cnt);
	lambda_chunk.SetCardinality(elem_cnt);

	// set the list child vector
	Vector slice(child_vector, sel, elem_cnt);
	slice.Normalify(elem_cnt);
	input_chunk.data[0].Reference(slice);

	// set the other vectors
	vector<Vector> slices;
	for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
		slices.emplace_back(Vector(args.data[col_idx + 1], sel_vectors[col_idx], elem_cnt));
		slices[col_idx].Normalify(elem_cnt);
		input_chunk.data[col_idx + 1].Reference(slices[col_idx]);
	}

	// execute the lambda expression
	expr_executor.Execute(input_chunk, lambda_chunk);
}

template <bool IS_TRANSFORM = true>
static void ListLambdaFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// always at least the list argument
	D_ASSERT(args.ColumnCount() >= 1);

	auto count = args.size();
	Vector &lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// get the lists data
	VectorData lists_data;
	lists.Orrify(count, lists_data);
	auto list_entries = (list_entry_t *)lists_data.data;

	// get the lambda expression
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ListLambdaBindData &)*func_expr.bind_info;
	auto &lambda_expr = info.lambda_expr;

	// get the child vector and child data
	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	VectorData child_data;
	child_vector.Orrify(lists_size, child_data);

	// to slice the child vector
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	// this vector never contains more than one element
	vector<LogicalType> result_types;
	result_types.push_back(lambda_expr->return_type);

	// non-lambda parameter columns
	vector<VectorData> columns;
	vector<idx_t> indexes;
	vector<SelectionVector> sel_vectors;

	vector<LogicalType> types;
	types.push_back(child_vector.GetType());

	// skip the list column
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		columns.emplace_back(VectorData());
		args.data[i].Orrify(count, columns[i - 1]);
		indexes.push_back(0);
		sel_vectors.emplace_back(SelectionVector(STANDARD_VECTOR_SIZE));
		types.push_back(args.data[i].GetType());
	}

	// get the expression executor
	ExpressionExecutor expr_executor(*lambda_expr);
	DataChunk input_chunk;
	DataChunk lambda_chunk;
	input_chunk.InitializeEmpty(types);
	lambda_chunk.Initialize(result_types);

	// these are only for the list_filter
	vector<idx_t> lists_len;
	idx_t curr_list_len = 0;
	idx_t curr_list_offset = 0;
	idx_t appended_lists_cnt = 0;
	idx_t curr_original_list_len = 0;

	if (!IS_TRANSFORM) {
		lists_len.reserve(count);
	}

	// loop over the child entries and create chunks to be executed by the expression executor
	idx_t elem_cnt = 0;
	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {

		auto lists_index = lists_data.sel->get_index(row_idx);
		const auto &list_entry = list_entries[lists_index];

		if (!IS_TRANSFORM) {
			lists_len.push_back(list_entry.length);
		}

		// set the result to NULL for this row
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(row_idx);
			continue;
		}

		// set the length and offset of the resulting lists of list_transform
		if (IS_TRANSFORM) {
			result_entries[row_idx].offset = offset;
			result_entries[row_idx].length = list_entry.length;
			offset += list_entry.length;
		}

		// empty list, nothing to execute
		if (list_entry.length == 0) {
			continue;
		}

		// get the data indexes
		for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
			indexes[col_idx] = columns[col_idx].sel->get_index(row_idx);
		}

		// iterate list elements and create transformed expression columns
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {

			// reached STANDARD_VECTOR_SIZE elements
			if (elem_cnt == STANDARD_VECTOR_SIZE) {

				ExecuteExpression(types, result_types, elem_cnt, sel, sel_vectors, input_chunk, lambda_chunk,
				                  child_vector, args, expr_executor);
				auto &lambda_vector = lambda_chunk.data[0];

				if (IS_TRANSFORM) {
					AppendTransformedToResult(lambda_vector, elem_cnt, result);
				} else {
					AppendFilteredToResult(lambda_vector, result_entries, elem_cnt, result, curr_list_len,
					                       curr_list_offset, appended_lists_cnt, lists_len, curr_original_list_len,
					                       input_chunk);
				}
				elem_cnt = 0;
			}

			// to slice the child vector
			auto source_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			sel.set_index(elem_cnt, source_idx);

			// for each column, set the index of the selection vector to slice properly
			for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
				sel_vectors[col_idx].set_index(elem_cnt, indexes[col_idx]);
			}
			elem_cnt++;
		}
	}

	ExecuteExpression(types, result_types, elem_cnt, sel, sel_vectors, input_chunk, lambda_chunk, child_vector, args,
	                  expr_executor);
	auto &lambda_vector = lambda_chunk.data[0];

	if (IS_TRANSFORM) {
		AppendTransformedToResult(lambda_vector, elem_cnt, result);
	} else {
		AppendFilteredToResult(lambda_vector, result_entries, elem_cnt, result, curr_list_len, curr_list_offset,
		                       appended_lists_cnt, lists_len, curr_original_list_len, input_chunk);
	}
}

static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ListLambdaFunction<>(args, state, result);
}

static void ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ListLambdaFunction<false>(args, state, result);
}

static void TransformExpression(unique_ptr<Expression> &original, unique_ptr<Expression> &replacement,
                                ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
                                LogicalType &list_child_type) {

	// check if the original expression is a lambda parameter
	bool is_lambda_parameter = false;
	if (original->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

		// determine if this is the lambda parameter
		auto &bound_col_ref = (BoundColumnRefExpression &)*original;
		if (bound_col_ref.binding.table_index == DConstants::INVALID_INDEX) {
			is_lambda_parameter = true;
		}
	}

	if (is_lambda_parameter) {
		// this is a lambda parameter, so the replacement refers to the first argument, which is the list
		replacement = make_unique<BoundReferenceExpression>(arguments[0]->alias, list_child_type, 0);

	} else {
		// this is not a lambda parameter, so we need to create a new argument for the arguments vector
		replacement = make_unique<BoundReferenceExpression>(original->alias, original->return_type, arguments.size());
		bound_function.arguments.push_back(original->return_type);
		arguments.push_back(move(original));
	}
}

static void IterateChildren(ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
                            LogicalType &list_child_type, unique_ptr<Expression> &expr) {

	if (expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
		throw InvalidInputException("Subqueries are not supported in lambda expressions!");
	}

	// these expression classes do not have children, transform them
	if (expr->expression_class == ExpressionClass::BOUND_CONSTANT ||
	    expr->expression_class == ExpressionClass::BOUND_COLUMN_REF ||
	    expr->expression_class == ExpressionClass::BOUND_DEFAULT ||
	    expr->expression_class == ExpressionClass::BOUND_PARAMETER ||
	    expr->expression_class == ExpressionClass::BOUND_REF) {

		// move the expr because we are going to replace it
		auto original = move(expr);
		unique_ptr<Expression> replacement;

		TransformExpression(original, replacement, bound_function, arguments, list_child_type);

		// replace the expression
		expr = move(replacement);

	} else {
		// recursively enumerate the children of the expression
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			IterateChildren(bound_function, arguments, list_child_type, child);
		});
	}
}

static unique_ptr<FunctionData> ListLambdaBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	// remove the lambda function
	auto lambda_expr = move(arguments.back());
	arguments.pop_back();
	bound_function.arguments.pop_back();

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	D_ASSERT(arguments[0]->return_type.id() == LogicalTypeId::LIST);
	auto list_child_type = ListType::GetChildType(arguments[0]->return_type);

	// iterate and transform the children of the lambda expression
	IterateChildren(bound_function, arguments, list_child_type, lambda_expr);

	// now the lambda expression has been modified, put it in the function data to use it during execution
	return make_unique<ListLambdaBindData>(bound_function.return_type, move(lambda_expr));
}

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the lambda function
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(arguments.size() == 2);

	bound_function.return_type = LogicalType::LIST(arguments[1]->return_type);
	return ListLambdaBind(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	// the list column and the lambda function
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(arguments.size() == 2);

	bound_function.return_type = arguments[0]->return_type;
	return ListLambdaBind(context, bound_function, arguments);
}

ScalarFunction ListTransformFun::GetFunction() {
	// the return type is a list of ANY, because it is the return value of the rhs of the lambda expression
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, LogicalType::LIST(LogicalType::ANY),
	                      ListTransformFunction, false, false, ListTransformBind, nullptr);
}

void ListTransformFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_transform", "array_transform", "list_apply", "array_apply"}, GetFunction());
}

ScalarFunction ListFilterFun::GetFunction() {
	// the return type is a list of ANY, because it is the return value of the rhs of the lambda expression
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, LogicalType::LIST(LogicalType::ANY),
	                      ListFilterFunction, false, false, ListFilterBind, nullptr);
}

void ListFilterFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_filter", "array_filter"}, GetFunction());
}

} // namespace duckdb