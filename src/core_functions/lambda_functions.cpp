#include "duckdb/core_functions/lambda_functions.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper functions
//===--------------------------------------------------------------------===//

struct LambdaColumnData {
	LambdaColumnData(const VectorType &vector_type, Vector &vector)
	    : is_const(vector_type == VectorType::CONSTANT_VECTOR), sel_vector(SelectionVector(STANDARD_VECTOR_SIZE)),
	      vector(vector) {};

	bool is_const;
	SelectionVector sel_vector;
	UnifiedVectorFormat format;
	reference<Vector> vector;
};

struct ExecuteExprData {
	ExecuteExprData(ClientContext &context, const Expression &lambda_expr, const DataChunk &args, const bool has_index,
	                const Vector &child_vector)
	    : has_index(has_index) {

		expr_executor = make_uniq<ExpressionExecutor>(context, lambda_expr);

		// get the input types
		vector<LogicalType> input_types;
		if (has_index) {
			input_types.push_back(LogicalType::BIGINT);
		}
		input_types.push_back(child_vector.GetType());
		for (idx_t i = 1; i < args.ColumnCount(); i++) {
			input_types.push_back(args.data[i].GetType());
		}

		// get the result types
		vector<LogicalType> result_types {lambda_expr.return_type};

		// initialize the data chunks
		input_chunk.InitializeEmpty(input_types);
		lambda_chunk.Initialize(Allocator::DefaultAllocator(), result_types);
	};

	DataChunk input_chunk;
	DataChunk lambda_chunk;
	unique_ptr<ExpressionExecutor> expr_executor;
	bool has_index;
};

struct ListFilterInfo {
	vector<idx_t> list_entry_lengths;
	idx_t curr_list_len = 0;
	idx_t curr_list_offset = 0;
	idx_t curr_child_entry_count = 0;
	idx_t curr_src_list_len = 0;
};

struct ListTransformFunctor {
	static void ReserveNewLengths(vector<idx_t> &, const idx_t) {
	}
	static void PushEmptyList(vector<idx_t> &) {
	}
	static void SetResultEntry(list_entry_t *result_entries, idx_t &offset, const list_entry_t &list_entry,
	                           const idx_t row_idx, vector<idx_t> &) {
		result_entries[row_idx].offset = offset;
		result_entries[row_idx].length = list_entry.length;
		offset += list_entry.length;
	}
	static void AppendResult(Vector &result, Vector &lambda_vector, const idx_t elem_cnt, list_entry_t *,
	                         ListFilterInfo &, ExecuteExprData &) {
		ListVector::Append(result, lambda_vector, elem_cnt, 0);
	}
};

struct ListFilterFunctor {
	static void ReserveNewLengths(vector<idx_t> &list_entry_lengths, const idx_t row_count) {
		list_entry_lengths.reserve(row_count);
	}
	static void PushEmptyList(vector<idx_t> &list_entry_lengths) {
		list_entry_lengths.emplace_back(0);
	}
	static void SetResultEntry(list_entry_t *, idx_t &, const list_entry_t &list_entry, const idx_t,
	                           vector<idx_t> &list_entry_lengths) {
		list_entry_lengths.push_back(list_entry.length);
	}
	static void AppendResult(Vector &result, Vector &lambda_vector, const idx_t elem_cnt, list_entry_t *result_entries,
	                         ListFilterInfo &info, ExecuteExprData &data) {

		idx_t true_count = 0;
		SelectionVector true_sel(elem_cnt);
		UnifiedVectorFormat lambda_data;
		lambda_vector.ToUnifiedFormat(elem_cnt, lambda_data);

		auto lambda_values = UnifiedVectorFormat::GetData<bool>(lambda_data);
		auto &lambda_validity = lambda_data.validity;

		// compute the new lengths and offsets, and create a selection vector
		for (idx_t i = 0; i < elem_cnt; i++) {
			auto entry = lambda_data.sel->get_index(i);

			while (info.curr_child_entry_count < info.list_entry_lengths.size() &&
			       info.list_entry_lengths[info.curr_child_entry_count] == 0) {
				result_entries[info.curr_child_entry_count].offset = info.curr_list_offset;
				result_entries[info.curr_child_entry_count].length = 0;
				info.curr_child_entry_count++;
			}

			// found a true value
			if (lambda_validity.RowIsValid(entry) && lambda_values[entry]) {
				true_sel.set_index(true_count++, i);
				info.curr_list_len++;
			}

			info.curr_src_list_len++;

			if (info.list_entry_lengths[info.curr_child_entry_count] == info.curr_src_list_len) {
				result_entries[info.curr_child_entry_count].offset = info.curr_list_offset;
				result_entries[info.curr_child_entry_count].length = info.curr_list_len;
				info.curr_list_offset += info.curr_list_len;
				info.curr_child_entry_count++;
				info.curr_list_len = 0;
				info.curr_src_list_len = 0;
			}
		}

		while (info.curr_child_entry_count < info.list_entry_lengths.size() &&
		       info.list_entry_lengths[info.curr_child_entry_count] == 0) {
			result_entries[info.curr_child_entry_count].offset = info.curr_list_offset;
			result_entries[info.curr_child_entry_count].length = 0;
			info.curr_child_entry_count++;
		}

		// slice to get the new lists and append them to the result
		auto new_lists_idx = data.has_index ? 1 : 0;
		Vector new_lists(data.input_chunk.data[new_lists_idx], true_sel, true_count);
		ListVector::Append(result, new_lists, true_count, 0);
	}
};

vector<LambdaColumnData> GetLambdaColumnData(DataChunk &args, idx_t row_count) {

	vector<LambdaColumnData> data;
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		data.emplace_back(args.data[i].GetVectorType(), args.data[i]);
		args.data[i].ToUnifiedFormat(row_count, data.back().format);
	}
	return data;
}

vector<reference<LambdaColumnData>> GetNonConstData(vector<LambdaColumnData> &data) {

	vector<reference<LambdaColumnData>> non_const_data;
	for (auto &entry : data) {
		if (!entry.is_const) {
			non_const_data.push_back(entry);
		}
	}
	return non_const_data;
}

void ExecuteExpression(idx_t &elem_cnt, LambdaColumnData &child_data, vector<LambdaColumnData> &column_data,
                       Vector &index_vector, ExecuteExprData &data) {

	data.input_chunk.SetCardinality(elem_cnt);
	data.lambda_chunk.SetCardinality(elem_cnt);

	// set the list child vector
	Vector slice(child_data.vector, child_data.sel_vector, elem_cnt);

	// check if the lambda expression has an index parameter
	if (data.has_index) {
		data.input_chunk.data[0].Reference(index_vector);
		data.input_chunk.data[1].Reference(slice);
	} else {
		data.input_chunk.data[0].Reference(slice);
	}
	idx_t slice_offset = data.has_index ? 2 : 1;

	// (slice and) reference the other columns
	vector<Vector> slices;
	for (idx_t i = 0; i < column_data.size(); i++) {
		if (column_data[i].is_const) {
			data.input_chunk.data[i + slice_offset].Reference(column_data[i].vector);
		} else {
			slices.emplace_back(column_data[i].vector, column_data[i].sel_vector, elem_cnt);
			data.input_chunk.data[i + slice_offset].Reference(slices.back());
		}
	}

	// execute the lambda expression
	data.expr_executor->Execute(data.input_chunk, data.lambda_chunk);
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
	auto &bind_data = bind_data_p->Cast<ListLambdaBindData>();
	serializer.WriteProperty(100, "return_type", bind_data.return_type);
	serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr, unique_ptr<Expression>());
	serializer.WriteProperty(102, "has_index", bind_data.has_index);
}

unique_ptr<FunctionData> ListLambdaBindData::Deserialize(Deserializer &deserializer, ScalarFunction &) {
	auto return_type = deserializer.ReadProperty<LogicalType>(100, "return_type");
	auto lambda_expr =
	    deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(101, "lambda_expr", unique_ptr<Expression>());
	auto has_index = deserializer.ReadProperty<bool>(102, "has_index");
	return make_uniq<ListLambdaBindData>(return_type, std::move(lambda_expr), has_index);
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

template <class FUNCTION_FUNCTOR>
void ExecuteLambda(DataChunk &args, ExpressionState &state, Vector &result) {

	auto row_count = args.size();
	Vector &list_column = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (list_column.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// get the lambda expression
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListLambdaBindData>();
	auto &lambda_expr = info.lambda_expr;
	bool has_side_effects = lambda_expr->HasSideEffects();

	// get the list column entries
	UnifiedVectorFormat list_column_format;
	list_column.ToUnifiedFormat(row_count, list_column_format);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_column_format);

	// special-handling for the child_vector
	auto child_vector_size = ListVector::GetListSize(list_column);
	auto &child_vector = ListVector::GetEntry(list_column);
	LambdaColumnData child_data(child_vector.GetVectorType(), child_vector);
	child_vector.ToUnifiedFormat(child_vector_size, child_data.format);

	// get the lambda column data for all other input vectors
	auto column_data = GetLambdaColumnData(args, row_count);
	auto non_const_column_data = GetNonConstData(column_data);

	// get the expression executor
	ExecuteExprData execute_expr_data(state.GetContext(), *lambda_expr, args, info.has_index, child_vector);

	// get list_filter specific info
	ListFilterInfo list_filter_info;
	FUNCTION_FUNCTOR::ReserveNewLengths(list_filter_info.list_entry_lengths, row_count);

	// additional index vector
	Vector index_vector(LogicalType::BIGINT);

	// loop over the child entries and create chunks to be executed by the expression executor
	idx_t elem_cnt = 0;
	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {

		auto list_row_idx = list_column_format.sel->get_index(row_idx);
		const auto &list_entry = list_entries[list_row_idx];

		// set the result to NULL for this row
		if (!list_column_format.validity.RowIsValid(list_row_idx)) {
			result_validity.SetInvalid(row_idx);
			FUNCTION_FUNCTOR::PushEmptyList(list_filter_info.list_entry_lengths);
			continue;
		}

		FUNCTION_FUNCTOR::SetResultEntry(result_entries, offset, list_entry, row_idx,
		                                 list_filter_info.list_entry_lengths);

		// empty list, nothing to execute
		if (list_entry.length == 0) {
			continue;
		}

		// iterate the elements of the current list and create the corresponding selection vectors
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {

			// reached STANDARD_VECTOR_SIZE elements
			if (elem_cnt == STANDARD_VECTOR_SIZE) {

				execute_expr_data.lambda_chunk.Reset();
				ExecuteExpression(elem_cnt, child_data, column_data, index_vector, execute_expr_data);
				auto &lambda_vector = execute_expr_data.lambda_chunk.data[0];

				FUNCTION_FUNCTOR::AppendResult(result, lambda_vector, elem_cnt, result_entries, list_filter_info,
				                               execute_expr_data);
				elem_cnt = 0;
			}

			// adjust indexes for slicing
			child_data.sel_vector.set_index(elem_cnt, list_entry.offset + child_idx);
			for (auto &entry : non_const_column_data) {
				entry.get().sel_vector.set_index(elem_cnt, row_idx);
			}

			// set the index vector
			if (info.has_index) {
				index_vector.SetValue(elem_cnt, Value::BIGINT(child_idx + 1));
			}

			elem_cnt++;
		}
	}

	execute_expr_data.lambda_chunk.Reset();
	ExecuteExpression(elem_cnt, child_data, column_data, index_vector, execute_expr_data);
	auto &lambda_vector = execute_expr_data.lambda_chunk.data[0];

	FUNCTION_FUNCTOR::AppendResult(result, lambda_vector, elem_cnt, result_entries, list_filter_info,
	                               execute_expr_data);

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

void LambdaFunctions::ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteLambda<ListTransformFunctor>(args, state, result);
}

void LambdaFunctions::ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteLambda<ListFilterFunctor>(args, state, result);
}

} // namespace duckdb
