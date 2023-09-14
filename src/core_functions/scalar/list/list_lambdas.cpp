#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

struct ListLambdaBindData : public FunctionData {
	ListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr);
	~ListLambdaBindData() override;

	LogicalType stype;
	unique_ptr<Expression> lambda_expr;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const ScalarFunction &function) {
		//		auto &bind_data = bind_data_p->Cast<ListLambdaBindData>();
		//		serializer.WriteProperty(100, "stype", bind_data.stype);
		//		serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr,
		// unique_ptr<Expression>());
		throw NotImplementedException("FIXME: list lambda serialize");
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &function) {
		//		auto stype = deserializer.ReadProperty<LogicalType>(100, "stype");
		//		auto lambda_expr =
		//		    deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(101, "lambda_expr",
		// unique_ptr<Expression>()); 		return make_uniq<ListLambdaBindData>(stype, std::move(lambda_expr));
		throw NotImplementedException("FIXME: list lambda deserialize");
	}
};

ListLambdaBindData::ListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr_p)
    : stype(stype_p), lambda_expr(std::move(lambda_expr_p)) {
}

unique_ptr<FunctionData> ListLambdaBindData::Copy() const {
	return make_uniq<ListLambdaBindData>(stype, lambda_expr ? lambda_expr->Copy() : nullptr);
}

bool ListLambdaBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ListLambdaBindData>();
	return Expression::Equals(lambda_expr, other.lambda_expr) && stype == other.stype;
}

ListLambdaBindData::~ListLambdaBindData() {
}

static void AppendTransformedToResult(Vector &lambda_vector, idx_t &elem_cnt, Vector &result) {

	// append the lambda_vector to the result list
	UnifiedVectorFormat lambda_child_data;
	lambda_vector.ToUnifiedFormat(elem_cnt, lambda_child_data);
	ListVector::Append(result, lambda_vector, *lambda_child_data.sel, elem_cnt, 0);
}

static void AppendFilteredToResult(Vector &lambda_vector, list_entry_t *result_entries, idx_t &elem_cnt, Vector &result,
                                   idx_t &curr_list_len, idx_t &curr_list_offset, idx_t &appended_lists_cnt,
                                   vector<idx_t> &lists_len, idx_t &curr_original_list_len, DataChunk &input_chunk) {

	idx_t true_count = 0;
	SelectionVector true_sel(elem_cnt);
	UnifiedVectorFormat lambda_data;
	lambda_vector.ToUnifiedFormat(elem_cnt, lambda_data);

	auto lambda_values = UnifiedVectorFormat::GetData<bool>(lambda_data);
	auto &lambda_validity = lambda_data.validity;

	// compute the new lengths and offsets, and create a selection vector
	for (idx_t i = 0; i < elem_cnt; i++) {
		auto entry = lambda_data.sel->get_index(i);

		while (appended_lists_cnt < lists_len.size() && lists_len[appended_lists_cnt] == 0) {
			result_entries[appended_lists_cnt].offset = curr_list_offset;
			result_entries[appended_lists_cnt].length = 0;
			appended_lists_cnt++;
		}

		// found a true value
		if (lambda_validity.RowIsValid(entry) && lambda_values[entry]) {
			true_sel.set_index(true_count++, i);
			curr_list_len++;
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

	while (appended_lists_cnt < lists_len.size() && lists_len[appended_lists_cnt] == 0) {
		result_entries[appended_lists_cnt].offset = curr_list_offset;
		result_entries[appended_lists_cnt].length = 0;
		appended_lists_cnt++;
	}

	// slice to get the new lists and append them to the result
	Vector new_lists(input_chunk.data[0], true_sel, true_count);
	new_lists.Flatten(true_count);
	UnifiedVectorFormat new_lists_child_data;
	new_lists.ToUnifiedFormat(true_count, new_lists_child_data);
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
	Vector second_slice(child_vector, sel, elem_cnt);
	slice.Flatten(elem_cnt);
	second_slice.Flatten(elem_cnt);

	input_chunk.data[0].Reference(slice);
	input_chunk.data[1].Reference(second_slice);

	// set the other vectors
	vector<Vector> slices;
	for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
		slices.emplace_back(args.data[col_idx + 1], sel_vectors[col_idx], elem_cnt);
		slices[col_idx].Flatten(elem_cnt);
		input_chunk.data[col_idx + 2].Reference(slices[col_idx]);
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

	// e.g. window functions in sub queries return dictionary vectors, which segfault on expression execution
	// if not flattened first
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::FLAT_VECTOR &&
		    args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			args.data[i].Flatten(count);
		}
	}

	// get the lists data
	UnifiedVectorFormat lists_data;
	lists.ToUnifiedFormat(count, lists_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

	// get the lambda expression
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListLambdaBindData>();
	auto &lambda_expr = info.lambda_expr;

	// get the child vector and child data
	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	child_vector.Flatten(lists_size);
	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(lists_size, child_data);

	// to slice the child vector
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	// this vector never contains more than one element
	vector<LogicalType> result_types;
	result_types.push_back(lambda_expr->return_type);

	// non-lambda parameter columns
	vector<UnifiedVectorFormat> columns;
	vector<idx_t> indexes;
	vector<SelectionVector> sel_vectors;

	vector<LogicalType> types;
	types.push_back(child_vector.GetType());
	types.push_back(child_vector.GetType());

	// skip the list column
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		columns.emplace_back();
		args.data[i].ToUnifiedFormat(count, columns[i - 1]);
		indexes.push_back(0);
		sel_vectors.emplace_back(STANDARD_VECTOR_SIZE);
		types.push_back(args.data[i].GetType());
	}

	// get the expression executor
	ExpressionExecutor expr_executor(state.GetContext(), *lambda_expr);

	// these are only for the list_filter
	vector<idx_t> lists_len;
	idx_t curr_list_len = 0;
	idx_t curr_list_offset = 0;
	idx_t appended_lists_cnt = 0;
	idx_t curr_original_list_len = 0;

	if (!IS_TRANSFORM) {
		lists_len.reserve(count);
	}

	DataChunk input_chunk;
	DataChunk lambda_chunk;
	input_chunk.InitializeEmpty(types);
	lambda_chunk.Initialize(Allocator::DefaultAllocator(), result_types);

	// loop over the child entries and create chunks to be executed by the expression executor
	idx_t elem_cnt = 0;
	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {

		auto lists_index = lists_data.sel->get_index(row_idx);
		const auto &list_entry = list_entries[lists_index];

		// set the result to NULL for this row
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(row_idx);
			if (!IS_TRANSFORM) {
				lists_len.push_back(0);
			}
			continue;
		}

		// set the length and offset of the resulting lists of list_transform
		if (IS_TRANSFORM) {
			result_entries[row_idx].offset = offset;
			result_entries[row_idx].length = list_entry.length;
			offset += list_entry.length;
		} else {
			lists_len.push_back(list_entry.length);
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
				lambda_chunk.Reset();
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

	lambda_chunk.Reset();
	ExecuteExpression(types, result_types, elem_cnt, sel, sel_vectors, input_chunk, lambda_chunk, child_vector, args,
	                  expr_executor);
	auto &lambda_vector = lambda_chunk.data[0];

	if (IS_TRANSFORM) {
		AppendTransformedToResult(lambda_vector, elem_cnt, result);
	} else {
		AppendFilteredToResult(lambda_vector, result_entries, elem_cnt, result, curr_list_len, curr_list_offset,
		                       appended_lists_cnt, lists_len, curr_original_list_len, input_chunk);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ListLambdaFunction<>(args, state, result);
}

static void ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ListLambdaFunction<false>(args, state, result);
}

template <int64_t LAMBDA_PARAM_CNT>
static unique_ptr<FunctionData> ListLambdaBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.parameter_count != LAMBDA_PARAM_CNT) {
		throw BinderException("Incorrect number of parameters in lambda function! " + bound_function.name +
		                      " expects " + to_string(LAMBDA_PARAM_CNT) + " parameter(s).");
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<ListLambdaBindData>(bound_function.return_type, nullptr);
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	D_ASSERT(arguments[0]->return_type.id() == LogicalTypeId::LIST);

	// get the lambda expression and put it in the bind info
	auto lambda_expr = std::move(bound_lambda_expr.lambda_expr);
	return make_uniq<ListLambdaBindData>(bound_function.return_type, std::move(lambda_expr));
}

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// at least the list column and the lambda function
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->expression_class != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	bound_function.return_type = LogicalType::LIST(bound_lambda_expr.lambda_expr->return_type);
	return ListLambdaBind<1>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	// at least the list column and the lambda function
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->expression_class != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	// try to cast to boolean, if the return type of the lambda filter expression is not already boolean
	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.lambda_expr->return_type != LogicalType::BOOLEAN) {
		auto cast_lambda_expr =
		    BoundCastExpression::AddCastToType(context, std::move(bound_lambda_expr.lambda_expr), LogicalType::BOOLEAN);
		bound_lambda_expr.lambda_expr = std::move(cast_lambda_expr);
	}

	bound_function.return_type = arguments[0]->return_type;
	return ListLambdaBind<1>(context, bound_function, arguments);
}

ScalarFunction ListTransformFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::LIST(LogicalType::ANY),
	                   ListTransformFunction, ListTransformBind, nullptr, nullptr);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	return fun;
}

ScalarFunction ListFilterFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::LIST(LogicalType::ANY),
	                   ListFilterFunction, ListFilterBind, nullptr, nullptr);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	return fun;
}

} // namespace duckdb
