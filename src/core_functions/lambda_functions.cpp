#include "duckdb/core_functions/lambda_functions.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper functions
//===--------------------------------------------------------------------===//

// FIXME: move these parameters into a struct/ more const
void ExecuteExpression(idx_t &elem_cnt, SelectionVector &sel, vector<SelectionVector> &sel_vectors,
                       DataChunk &input_chunk, DataChunk &lambda_chunk, Vector &child_vector, DataChunk &args,
                       ExpressionExecutor &expr_executor, const bool has_index, Vector &index_vector) {

	input_chunk.SetCardinality(elem_cnt);
	lambda_chunk.SetCardinality(elem_cnt);

	// set the list child vector
	// FIXME: no more flatten, we should be able to use dictionary vectors
	Vector slice(child_vector, sel, elem_cnt);
	slice.Flatten(elem_cnt);

	// check if the lambda expression has an index parameter
	// FIXME: this has to be more generic, only LIST_TRANSFORM and LIST_FILTER have the index as their
	// FIXME: second argument, LIST_REDUCE will have it as its third argument, and its always the first
	// FIXME: bound index, if it exists
	if (has_index) {
		input_chunk.data[0].Reference(index_vector);
		input_chunk.data[1].Reference(slice);
	} else {
		input_chunk.data[0].Reference(slice);
	}
	idx_t slice_offset = has_index ? 2 : 1;

	// set the other vectors (outer lambdas and captures)
	vector<Vector> slices;
	for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
		slices.emplace_back(args.data[col_idx + 1], sel_vectors[col_idx], elem_cnt);
		// FIXME: no more flatten, we should be able to use dictionary vectors
		slices[col_idx].Flatten(elem_cnt);
		input_chunk.data[col_idx + slice_offset].Reference(slices[col_idx]);
	}

	// execute the lambda expression
	expr_executor.Execute(input_chunk, lambda_chunk);
}

void AppendTransformedToResult(Vector &lambda_vector, idx_t &elem_cnt, Vector &result) {

	// append the lambda_vector to the result list
	UnifiedVectorFormat lambda_child_data;
	lambda_vector.ToUnifiedFormat(elem_cnt, lambda_child_data);
	ListVector::Append(result, lambda_vector, *lambda_child_data.sel, elem_cnt, 0);
}

// FIXME: move these parameters into a struct/ more const
void AppendFilteredToResult(Vector &lambda_vector, list_entry_t *result_entries, idx_t &elem_cnt, Vector &result,
                            idx_t &curr_list_len, idx_t &curr_list_offset, idx_t &appended_lists_cnt,
                            vector<idx_t> &lists_len, idx_t &curr_original_list_len, DataChunk &input_chunk,
                            const bool has_index) {

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
	auto new_lists_idx = has_index ? 1 : 0;
	Vector new_lists(input_chunk.data[new_lists_idx], true_sel, true_count);
	// FIXME: no more flatten, we should be able to use dictionary vectors
	new_lists.Flatten(true_count);
	UnifiedVectorFormat new_lists_child_data;
	new_lists.ToUnifiedFormat(true_count, new_lists_child_data);
	ListVector::Append(result, new_lists, *new_lists_child_data.sel, true_count, 0);
}

//===--------------------------------------------------------------------===//
// ListLambdaBindData
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> ListLambdaBindData::Copy() const {
	auto lambda_expr_copy = lambda_expr ? lambda_expr->Copy() : nullptr;
	return make_uniq<ListLambdaBindData>(return_type, std::move(lambda_expr_copy), has_index);
}

bool ListLambdaBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ListLambdaBindData>();
	return Expression::Equals(lambda_expr, other.lambda_expr) && return_type == other.return_type &&
	       has_index == other.has_index;
}

void ListLambdaBindData::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                   const ScalarFunction &) {
	//	auto &bind_data = bind_data_p->Cast<ListLambdaBindData>();
	//	serializer.WriteProperty(100, "return_type", bind_data.return_type);
	//	serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr, unique_ptr<Expression>());
	throw NotImplementedException("FIXME: list lambda serialize");
}

unique_ptr<FunctionData> ListLambdaBindData::Deserialize(Deserializer &deserializer, ScalarFunction &) {
	//	auto return_type = deserializer.ReadProperty<LogicalType>(100, "return_type");
	//	auto lambda_expr =
	//	    deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(101, "lambda_expr", unique_ptr<Expression>());
	//	return make_uniq<ListLambdaBindData>(return_type, std::move(lambda_expr));
	throw NotImplementedException("FIXME: list lambda deserialize");
}

//===--------------------------------------------------------------------===//
// LambdaFunctions
//===--------------------------------------------------------------------===//

LogicalType LambdaFunctions::BindBinaryLambda(const idx_t parameter_idx, const LogicalType &list_child_type) {
	switch (parameter_idx) {
	case 0:
		return list_child_type;
	case 1:
		return LogicalType::BIGINT;
	default:
		throw BinderException("This lambda function only supports up to two lambda parameters!");
	}
}

void LambdaFunctions::ExecuteLambda(DataChunk &args, ExpressionState &state, Vector &result, LambdaType lambda_type) {

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
	// FIXME: no more flatten, we should be able to use dictionary vectors (on top of these dictionary vectors?)
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
	bool has_side_effects = lambda_expr->HasSideEffects();

	// get the child vector and child data
	// FIXME: no more flatten, we should be able to use dictionary vectors
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

	// FIXME: currently, we also slice/flatten constant vectors, which should not be necessary
	// outer lambdas and captures
	vector<UnifiedVectorFormat> columns;
	vector<idx_t> indexes;
	vector<SelectionVector> sel_vectors;
	vector<LogicalType> types;

	// FIXME: more generic? or can we just assume the index to be always bound to zero, if it exists?
	if (info.has_index) {
		// binary lambda function with an index
		types.push_back(LogicalType::BIGINT);
	}
	// the current child vector
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

	// FIXME: this function should be more generic and not contain these if-else code paths
	// FIXME: for different scalar functions
	// these are only for LIST_FILTER
	vector<idx_t> lists_len;
	idx_t curr_list_len = 0;
	idx_t curr_list_offset = 0;
	idx_t appended_lists_cnt = 0;
	idx_t curr_original_list_len = 0;

	if (lambda_type != LambdaType::TRANSFORM) {
		lists_len.reserve(count);
	}

	DataChunk input_chunk;
	DataChunk lambda_chunk;
	input_chunk.InitializeEmpty(types);
	lambda_chunk.Initialize(Allocator::DefaultAllocator(), result_types);

	// additional index vector
	Vector index_vector(LogicalType::BIGINT);

	// loop over the child entries and create chunks to be executed by the expression executor
	idx_t elem_cnt = 0;
	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {

		auto lists_index = lists_data.sel->get_index(row_idx);
		const auto &list_entry = list_entries[lists_index];

		// set the result to NULL for this row
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(row_idx);
			if (lambda_type != LambdaType::TRANSFORM) {
				lists_len.push_back(0);
			}
			continue;
		}

		// set the length and offset of the resulting lists of list_transform
		if (lambda_type == LambdaType::TRANSFORM) {
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
				ExecuteExpression(elem_cnt, sel, sel_vectors, input_chunk, lambda_chunk, child_vector, args,
				                  expr_executor, info.has_index, index_vector);

				auto &lambda_vector = lambda_chunk.data[0];

				if (lambda_type == LambdaType::TRANSFORM) {
					AppendTransformedToResult(lambda_vector, elem_cnt, result);
				} else {
					AppendFilteredToResult(lambda_vector, result_entries, elem_cnt, result, curr_list_len,
					                       curr_list_offset, appended_lists_cnt, lists_len, curr_original_list_len,
					                       input_chunk, info.has_index);
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

			// adjust the index vector
			if (info.has_index) {
				index_vector.SetValue(elem_cnt, Value::BIGINT(child_idx + 1));
			}

			elem_cnt++;
		}
	}

	lambda_chunk.Reset();
	ExecuteExpression(elem_cnt, sel, sel_vectors, input_chunk, lambda_chunk, child_vector, args, expr_executor,
	                  info.has_index, index_vector);
	auto &lambda_vector = lambda_chunk.data[0];

	if (lambda_type == LambdaType::TRANSFORM) {
		AppendTransformedToResult(lambda_vector, elem_cnt, result);
	} else {
		AppendFilteredToResult(lambda_vector, result_entries, elem_cnt, result, curr_list_len, curr_list_offset,
		                       appended_lists_cnt, lists_len, curr_original_list_len, input_chunk, info.has_index);
	}

	if (args.AllConstant() && !has_side_effects) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

unique_ptr<FunctionData> LambdaFunctions::ListLambdaBind(ClientContext &context, ScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments,
                                                         const bool has_index) {

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<ListLambdaBindData>(bound_function.return_type, nullptr);
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	D_ASSERT(arguments[0]->return_type.id() == LogicalTypeId::LIST);

	// get the lambda expression and put it in the bind info
	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	auto lambda_expr = std::move(bound_lambda_expr.lambda_expr);
	return make_uniq<ListLambdaBindData>(bound_function.return_type, std::move(lambda_expr), has_index);
}

} // namespace duckdb
