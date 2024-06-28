#include "duckdb/core_functions/lambda_functions.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper functions
//===--------------------------------------------------------------------===//

//! LambdaExecuteInfo holds information for executing the lambda expression on an input chunk and
//! a resulting lambda chunk.
struct LambdaExecuteInfo {
	LambdaExecuteInfo(ClientContext &context, const Expression &lambda_expr, const DataChunk &args,
	                  const bool has_index, const Vector &child_vector)
	    : has_index(has_index) {

		expr_executor = make_uniq<ExpressionExecutor>(context, lambda_expr);

		// get the input types for the input chunk
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

	//! The expression executor that executes the lambda expression
	unique_ptr<ExpressionExecutor> expr_executor;
	//! The input chunk on which we execute the lambda expression
	DataChunk input_chunk;
	//! The chunk holding the result of executing the lambda expression
	DataChunk lambda_chunk;
	//! True, if this lambda expression expects an index vector in the input chunk
	bool has_index;
};

//! A helper struct with information that is specific to the list_filter function
struct ListFilterInfo {
	//! The new list lengths after filtering out elements
	vector<idx_t> entry_lengths;
	//! The length of the current list
	idx_t length = 0;
	//! The offset of the current list
	idx_t offset = 0;
	//! The current row index
	idx_t row_idx = 0;
	//! The length of the source list
	idx_t src_length = 0;
};

//! ListTransformFunctor contains list_transform specific functionality
struct ListTransformFunctor {
	static void ReserveNewLengths(vector<idx_t> &, const idx_t) {
		// NOP
	}
	static void PushEmptyList(vector<idx_t> &) {
		// NOP
	}
	//! Sets the list entries of the result vector
	static void SetResultEntry(list_entry_t *result_entries, idx_t &offset, const list_entry_t &entry,
	                           const idx_t row_idx, vector<idx_t> &) {
		result_entries[row_idx].offset = offset;
		result_entries[row_idx].length = entry.length;
		offset += entry.length;
	}
	//! Appends the lambda vector to the result's child vector
	static void AppendResult(Vector &result, Vector &lambda_vector, const idx_t elem_cnt, list_entry_t *,
	                         ListFilterInfo &, LambdaExecuteInfo &) {
		ListVector::Append(result, lambda_vector, elem_cnt, 0);
	}
};

//! ListFilterFunctor contains list_filter specific functionality
struct ListFilterFunctor {
	//! Initializes the entry_lengths vector
	static void ReserveNewLengths(vector<idx_t> &entry_lengths, const idx_t row_count) {
		entry_lengths.reserve(row_count);
	}
	//! Pushes an empty list to the entry_lengths vector
	static void PushEmptyList(vector<idx_t> &entry_lengths) {
		entry_lengths.emplace_back(0);
	}
	//! Pushes the length of the original list to the entry_lengths vector
	static void SetResultEntry(list_entry_t *, idx_t &, const list_entry_t &entry, const idx_t,
	                           vector<idx_t> &entry_lengths) {
		entry_lengths.push_back(entry.length);
	}
	//! Uses the lambda vector to filter the incoming list and to append the filtered list to the result vector
	static void AppendResult(Vector &result, Vector &lambda_vector, const idx_t elem_cnt, list_entry_t *result_entries,
	                         ListFilterInfo &info, LambdaExecuteInfo &execute_info) {

		idx_t count = 0;
		SelectionVector sel(elem_cnt);
		UnifiedVectorFormat lambda_data;
		lambda_vector.ToUnifiedFormat(elem_cnt, lambda_data);

		auto lambda_values = UnifiedVectorFormat::GetData<bool>(lambda_data);
		auto &lambda_validity = lambda_data.validity;

		// compute the new lengths and offsets, and create a selection vector
		for (idx_t i = 0; i < elem_cnt; i++) {
			auto entry_idx = lambda_data.sel->get_index(i);

			// set length and offset of empty lists
			while (info.row_idx < info.entry_lengths.size() && !info.entry_lengths[info.row_idx]) {
				result_entries[info.row_idx].offset = info.offset;
				result_entries[info.row_idx].length = 0;
				info.row_idx++;
			}

			// found a true value
			if (lambda_validity.RowIsValid(entry_idx) && lambda_values[entry_idx]) {
				sel.set_index(count++, i);
				info.length++;
			}

			info.src_length++;

			// we traversed the entire source list
			if (info.entry_lengths[info.row_idx] == info.src_length) {
				// set the offset and length of the result entry
				result_entries[info.row_idx].offset = info.offset;
				result_entries[info.row_idx].length = info.length;

				// reset all other fields
				info.offset += info.length;
				info.row_idx++;
				info.length = 0;
				info.src_length = 0;
			}
		}

		// set length and offset of all remaining empty lists
		while (info.row_idx < info.entry_lengths.size() && !info.entry_lengths[info.row_idx]) {
			result_entries[info.row_idx].offset = info.offset;
			result_entries[info.row_idx].length = 0;
			info.row_idx++;
		}

		// slice the input chunk's corresponding vector to get the new lists
		// and append them to the result
		idx_t source_list_idx = execute_info.has_index ? 1 : 0;
		Vector result_lists(execute_info.input_chunk.data[source_list_idx], sel, count);
		ListVector::Append(result, result_lists, count, 0);
	}
};

vector<LambdaFunctions::ColumnInfo> LambdaFunctions::GetColumnInfo(DataChunk &args, const idx_t row_count) {

	vector<ColumnInfo> data;
	// skip the input list and then insert all remaining input vectors
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		data.emplace_back(args.data[i]);
		args.data[i].ToUnifiedFormat(row_count, data.back().format);
	}
	return data;
}

vector<reference<LambdaFunctions::ColumnInfo>>
LambdaFunctions::GetInconstantColumnInfo(vector<LambdaFunctions::ColumnInfo> &data) {

	vector<reference<ColumnInfo>> inconstant_info;
	for (auto &entry : data) {
		if (entry.vector.get().GetVectorType() != VectorType::CONSTANT_VECTOR) {
			inconstant_info.push_back(entry);
		}
	}
	return inconstant_info;
}

void ExecuteExpression(const idx_t elem_cnt, const LambdaFunctions::ColumnInfo &column_info,
                       const vector<LambdaFunctions::ColumnInfo> &column_infos, const Vector &index_vector,
                       LambdaExecuteInfo &info) {

	info.input_chunk.SetCardinality(elem_cnt);
	info.lambda_chunk.SetCardinality(elem_cnt);

	// slice the child vector
	Vector slice(column_info.vector, column_info.sel, elem_cnt);

	// reference the child vector (and the index vector)
	if (info.has_index) {
		info.input_chunk.data[0].Reference(index_vector);
		info.input_chunk.data[1].Reference(slice);
	} else {
		info.input_chunk.data[0].Reference(slice);
	}
	idx_t slice_offset = info.has_index ? 2 : 1;

	// (slice and) reference the other columns
	vector<Vector> slices;
	for (idx_t i = 0; i < column_infos.size(); i++) {

		if (column_infos[i].vector.get().GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// only reference constant vectorsl
			info.input_chunk.data[i + slice_offset].Reference(column_infos[i].vector);

		} else {
			// slice inconstant vectors
			slices.emplace_back(column_infos[i].vector, column_infos[i].sel, elem_cnt);
			info.input_chunk.data[i + slice_offset].Reference(slices.back());
		}
	}

	// execute the lambda expression
	info.expr_executor->Execute(info.input_chunk, info.lambda_chunk);
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

LogicalType LambdaFunctions::BindTernaryLambda(const idx_t parameter_idx, const LogicalType &list_child_type) {
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

template <class FUNCTION_FUNCTOR>
void ExecuteLambda(DataChunk &args, ExpressionState &state, Vector &result) {

	bool result_is_null = false;
	LambdaFunctions::LambdaInfo info(args, state, result, result_is_null);
	if (result_is_null) {
		return;
	}

	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto inconstant_column_infos = LambdaFunctions::GetInconstantColumnInfo(info.column_infos);

	// special-handling for the child_vector
	auto child_vector_size = ListVector::GetListSize(args.data[0]);
	LambdaFunctions::ColumnInfo child_info(*info.child_vector);
	info.child_vector->ToUnifiedFormat(child_vector_size, child_info.format);

	// get the expression executor
	LambdaExecuteInfo execute_info(state.GetContext(), *info.lambda_expr, args, info.has_index, *info.child_vector);

	// get list_filter specific info
	ListFilterInfo list_filter_info;
	FUNCTION_FUNCTOR::ReserveNewLengths(list_filter_info.entry_lengths, info.row_count);

	// additional index vector
	Vector index_vector(LogicalType::BIGINT);

	// loop over the child entries and create chunks to be executed by the expression executor
	idx_t elem_cnt = 0;
	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < info.row_count; row_idx++) {

		auto list_idx = info.list_column_format.sel->get_index(row_idx);
		const auto &list_entry = info.list_entries[list_idx];

		// set the result to NULL for this row
		if (!info.list_column_format.validity.RowIsValid(list_idx)) {
			info.result_validity->SetInvalid(row_idx);
			FUNCTION_FUNCTOR::PushEmptyList(list_filter_info.entry_lengths);
			continue;
		}

		FUNCTION_FUNCTOR::SetResultEntry(result_entries, offset, list_entry, row_idx, list_filter_info.entry_lengths);

		// empty list, nothing to execute
		if (list_entry.length == 0) {
			continue;
		}

		// iterate the elements of the current list and create the corresponding selection vectors
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {

			// reached STANDARD_VECTOR_SIZE elements
			if (elem_cnt == STANDARD_VECTOR_SIZE) {

				execute_info.lambda_chunk.Reset();
				ExecuteExpression(elem_cnt, child_info, info.column_infos, index_vector, execute_info);
				auto &lambda_vector = execute_info.lambda_chunk.data[0];

				FUNCTION_FUNCTOR::AppendResult(result, lambda_vector, elem_cnt, result_entries, list_filter_info,
				                               execute_info);
				elem_cnt = 0;
			}

			// FIXME: reuse same selection vector for inconstant rows
			// adjust indexes for slicing
			child_info.sel.set_index(elem_cnt, list_entry.offset + child_idx);
			for (auto &entry : inconstant_column_infos) {
				entry.get().sel.set_index(elem_cnt, row_idx);
			}

			// set the index vector
			if (info.has_index) {
				index_vector.SetValue(elem_cnt, Value::BIGINT(NumericCast<int64_t>(child_idx + 1)));
			}

			elem_cnt++;
		}
	}

	execute_info.lambda_chunk.Reset();
	ExecuteExpression(elem_cnt, child_info, info.column_infos, index_vector, execute_info);
	auto &lambda_vector = execute_info.lambda_chunk.data[0];

	FUNCTION_FUNCTOR::AppendResult(result, lambda_vector, elem_cnt, result_entries, list_filter_info, execute_info);

	if (info.is_all_constant && !info.is_volatile) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

unique_ptr<FunctionData> LambdaFunctions::ListLambdaPrepareBind(vector<unique_ptr<Expression>> &arguments,
                                                                ClientContext &context,
                                                                ScalarFunction &bound_function) {
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
	return nullptr;
}

unique_ptr<FunctionData> LambdaFunctions::ListLambdaBind(ClientContext &context, ScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments,
                                                         const bool has_index) {
	unique_ptr<FunctionData> bind_data = ListLambdaPrepareBind(arguments, context, bound_function);
	if (bind_data) {
		return bind_data;
	}

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
