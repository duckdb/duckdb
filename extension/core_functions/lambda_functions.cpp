#include "duckdb/function/lambda_functions.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/common/enums/dialect_compatibility_mode.hpp"
#include "duckdb/main/settings.hpp"

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"

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
		vector<LogicalType> result_types {lambda_expr.GetReturnType()};

		// initialize the data chunks
		input_chunk.InitializeEmpty(input_types);
		lambda_chunk.Initialize(Allocator::DefaultAllocator(), result_types);
		// Spark Compatibility Mode: zero-based index for lambdas
		if (Settings::Get<DialectCompatibilityModeSetting>(context) == DialectCompatibilityMode::SPARK) {
			// Spark's lambda index parameter is 0-based; default SQL is 1-based
			index_offset = 0;
		}
	};

	//! The expression executor that executes the lambda expression
	unique_ptr<ExpressionExecutor> expr_executor;
	//! The input chunk on which we execute the lambda expression
	DataChunk input_chunk;
	//! The chunk holding the result of executing the lambda expression
	DataChunk lambda_chunk;
	//! True, if this lambda expression expects an index vector in the input chunk
	bool has_index;
	//! Added to child_idx to form the value the lambda sees in its index parameter (1 by default).
	idx_t index_offset = 1;
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
	static void AppendResult(Vector &result, const Vector &lambda_vector, const idx_t elem_cnt, list_entry_t *,
	                         ListFilterInfo &, LambdaExecuteInfo &) {
		ListVector::Append(result, lambda_vector, elem_cnt);
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
	static void AppendResult(Vector &result, const Vector &lambda_vector, const idx_t elem_cnt,
	                         list_entry_t *result_entries, ListFilterInfo &info, LambdaExecuteInfo &execute_info) {
		idx_t count = 0;
		SelectionVector sel(elem_cnt);

		// compute the new lengths and offsets, and create a selection vector
		for (auto entry : lambda_vector.Values<bool>()) {
			// set length and offset of empty lists
			while (info.row_idx < info.entry_lengths.size() && !info.entry_lengths[info.row_idx]) {
				result_entries[info.row_idx].offset = info.offset;
				result_entries[info.row_idx].length = 0;
				info.row_idx++;
			}

			// found a true value
			if (entry.IsValid() && entry.GetValue()) {
				sel.set_index(count++, entry.GetIndex());
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
		ListVector::Append(result, result_lists, count);
	}
};

vector<LambdaFunctions::ColumnInfo> LambdaFunctions::GetColumnInfo(DataChunk &args, const idx_t row_count) {
	vector<ColumnInfo> data;
	// skip the input list and then insert all remaining input vectors
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		data.emplace_back(args.data[i]);
		args.data[i].ToUnifiedFormat(data.back().format);
	}
	return data;
}

vector<reference<LambdaFunctions::ColumnInfo>>
LambdaFunctions::GetMutableColumnInfo(vector<LambdaFunctions::ColumnInfo> &data) {
	vector<reference<ColumnInfo>> inconstant_info;
	for (auto &entry : data) {
		if (entry.vector.get().GetVectorType() != VectorType::CONSTANT_VECTOR) {
			inconstant_info.push_back(entry);
		}
	}
	return inconstant_info;
}

static void ExecuteExpression(const idx_t elem_cnt, const LambdaFunctions::ColumnInfo &column_info,
                              const vector<LambdaFunctions::ColumnInfo> &column_infos, const Vector &index_vector,
                              LambdaExecuteInfo &info) {
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
		slices.emplace_back(column_infos[i].vector, column_infos[i].sel, elem_cnt);
		info.input_chunk.data[i + slice_offset].Reference(slices.back());
	}

	// ensure all input vectors are sized to the chunk cardinality (some references inherit a different size)
	info.input_chunk.SetChildCardinality(elem_cnt);

	// execute the lambda expression
	info.expr_executor->Execute(info.input_chunk, info.lambda_chunk);
}

//===--------------------------------------------------------------------===//
// ListLambdaBindData
//===--------------------------------------------------------------------===//

void ListLambdaBindData::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                   const BoundScalarFunction &) {
	auto &bind_data = bind_data_p->Cast<ListLambdaBindData>();
	serializer.WriteProperty(100, "return_type", bind_data.return_type);
	serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr, unique_ptr<Expression>());
	serializer.WriteProperty(102, "has_index", bind_data.has_index);
	serializer.WritePropertyWithDefault<bool>(103, "has_initial", bind_data.has_initial, false);
	serializer.WritePropertyWithDefault<idx_t>(104, "parameter_count", bind_data.parameter_count, 0);
	serializer.WritePropertyWithDefault<idx_t>(105, "capture_count", bind_data.capture_count, 0);
	serializer.WritePropertyWithDefault<idx_t>(106, "element_ref_index", bind_data.element_ref_index,
	                                           DConstants::INVALID_INDEX);
}

unique_ptr<FunctionData> ListLambdaBindData::Deserialize(Deserializer &deserializer, BoundScalarFunction &) {
	auto return_type = deserializer.ReadProperty<LogicalType>(100, "return_type");
	auto lambda_expr = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<Expression>>(101, "lambda_expr",
	                                                                                        unique_ptr<Expression>());
	auto has_index = deserializer.ReadProperty<bool>(102, "has_index");
	auto has_initial = deserializer.ReadPropertyWithExplicitDefault<bool>(103, "has_initial", false);
	auto parameter_count = deserializer.ReadPropertyWithExplicitDefault<idx_t>(104, "parameter_count", 0);
	auto capture_count = deserializer.ReadPropertyWithExplicitDefault<idx_t>(105, "capture_count", 0);
	auto element_ref_index =
	    deserializer.ReadPropertyWithExplicitDefault<idx_t>(106, "element_ref_index", DConstants::INVALID_INDEX);
	return make_uniq<ListLambdaBindData>(return_type, std::move(lambda_expr), has_index, has_initial, parameter_count,
	                                     capture_count, element_ref_index);
}

//===--------------------------------------------------------------------===//
// LambdaFunctions
//===--------------------------------------------------------------------===//
LogicalType LambdaFunctions::DetermineListChildType(const LogicalType &child_type) {
	if (child_type.id() != LogicalTypeId::SQLNULL && child_type.id() != LogicalTypeId::UNKNOWN) {
		if (child_type.id() == LogicalTypeId::ARRAY) {
			return ArrayType::GetChildType(child_type);
		} else if (child_type.id() == LogicalTypeId::LIST) {
			return ListType::GetChildType(child_type);
		}
		throw InternalException("The first argument must be a list or array type");
	}

	return child_type;
}

LogicalType LambdaFunctions::BindBinaryChildren(const vector<LogicalType> &function_child_types,
                                                const idx_t parameter_idx) {
	auto list_type = DetermineListChildType(function_child_types[0]);

	switch (parameter_idx) {
	case 0:
		return list_type;
	case 1:
		return LogicalType::BIGINT;
	default:
		throw BinderException("This lambda function only supports up to two lambda parameters!");
	}
}

template <class FUNCTION_FUNCTOR>
static void ExecuteLambda(DataChunk &args, ExpressionState &state, Vector &result) {
	bool result_is_null = false;
	LambdaFunctions::LambdaInfo info(args, state, result, result_is_null);
	if (result_is_null) {
		return;
	}

	auto result_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto mutable_column_infos = LambdaFunctions::GetMutableColumnInfo(info.column_infos);

	// special-handling for the child_vector
	LambdaFunctions::ColumnInfo child_info(*info.child_vector);
	info.child_vector->ToUnifiedFormat(child_info.format);

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
			for (auto &entry : mutable_column_infos) {
				entry.get().sel.set_index(elem_cnt, row_idx);
			}

			// set the index vector
			if (info.has_index) {
				index_vector.SetValue(elem_cnt,
				                      Value::BIGINT(NumericCast<int64_t>(child_idx + execute_info.index_offset)));
			}

			elem_cnt++;
		}
	}

	execute_info.lambda_chunk.Reset();
	if (elem_cnt > 0) {
		// only execute when there are remaining list elements; calling with elem_cnt = 0 would
		// resize the (shared) source buffers down to size 0
		ExecuteExpression(elem_cnt, child_info, info.column_infos, index_vector, execute_info);
	}
	auto &lambda_vector = execute_info.lambda_chunk.data[0];

	FUNCTION_FUNCTOR::AppendResult(result, lambda_vector, elem_cnt, result_entries, list_filter_info, execute_info);

	if (info.is_all_constant && !info.is_volatile) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	FlatVector::SetSize(result, count_t(info.row_count));
}

unique_ptr<FunctionData> LambdaFunctions::ListLambdaPrepareBind(vector<unique_ptr<Expression>> &arguments,
                                                                ClientContext &context,
                                                                BoundScalarFunction &bound_function) {
	// NULL list parameter
	if (arguments[0]->GetReturnType().id() == LogicalTypeId::SQLNULL) {
		bound_function.GetArguments()[0] = LogicalType::SQLNULL;
		bound_function.SetReturnType(LogicalType::SQLNULL);
		return make_uniq<ListLambdaBindData>(bound_function.GetReturnType(), nullptr);
	}
	// prepared statements
	if (arguments[0]->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	D_ASSERT(arguments[0]->GetReturnType().id() == LogicalTypeId::LIST);
	return nullptr;
}

unique_ptr<FunctionData> LambdaFunctions::ListLambdaBind(ClientContext &context, BoundScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments,
                                                         const bool has_index) {
	unique_ptr<FunctionData> bind_data = ListLambdaPrepareBind(arguments, context, bound_function);
	if (bind_data) {
		return bind_data;
	}

	// get the lambda expression and put it in the bind info
	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	auto parameter_count = bound_lambda_expr.ParameterCount();
	auto capture_count = bound_lambda_expr.Captures().size();
	// for list_transform/list_filter the list element is the first lambda parameter (column 0), so its body
	// reference index is parameter_count - 1 (see LambdaFunctions::BindBinaryChildren)
	auto element_ref_index = parameter_count - 1;
	auto lambda_expr = std::move(bound_lambda_expr.LambdaExprMutable());
	if (lambda_expr->IsVolatile()) {
		bound_function.SetVolatile();
	}

	return make_uniq<ListLambdaBindData>(bound_function.GetReturnType(), std::move(lambda_expr), has_index, false,
	                                     parameter_count, capture_count, element_ref_index);
}

unique_ptr<BaseStatistics> LambdaFunctions::ListLambdaStats(ClientContext &context, FunctionStatisticsInput &input) {
	// Lambda bodies are executed on a fresh ExpressionExecutor over selection-vector slices that carry no
	// statistics, so statistics-driven scalar fast paths (e.g. substring's ASCII path) are never selected inside a
	// lambda. Seed the lambda body's reference statistics here - from the (already propagated) statistics of the
	// list element and the captured columns - so the optimizer's statistics propagation reaches inside the body.
	// NOTE: any resulting function-callback swap (e.g. SetFunctionCallback) lives on the in-memory lambda body and
	// is not serialized; a plan serialized after this pass and re-deserialized (e.g. distributed execution) reverts
	// to the generic path, exactly as top-level substring does. This is correct, just unoptimized, on that path.
	if (!input.propagator || !input.bind_data) {
		return nullptr;
	}
	auto &bind_data = input.bind_data->Cast<ListLambdaBindData>();
	if (!bind_data.lambda_expr) {
		return nullptr;
	}
	auto &child_stats = input.child_stats;
	// child_stats holds the list argument plus the non-list children; captures are a subset of the latter, so we
	// need at least capture_count + 1 entries (unsigned comparison, no subtraction => no underflow trap)
	if (child_stats.size() <= bind_data.capture_count) {
		return nullptr;
	}

	// children layout = [list, (initial value?), (nested lambda params...), captures...]. The captures are the last
	// capture_count children and map to contiguous body reference indices starting at parameter_count + nested_count
	// - the same layout the lambda executor builds at runtime.
	idx_t base_child = child_stats.size() - bind_data.capture_count;
	idx_t initial_count = bind_data.has_initial ? 1 : 0;
	if (base_child < 1 + initial_count) {
		// defensive: inconsistent child layout, do not risk seeding incorrect statistics
		return nullptr;
	}
	idx_t nested_count = base_child - 1 - initial_count;
	idx_t capture_ref_base = bind_data.parameter_count + nested_count;

	// size to cover both the capture references and the (normally smaller) element reference
	idx_t ref_count = capture_ref_base + bind_data.capture_count;
	if (bind_data.element_ref_index != DConstants::INVALID_INDEX && bind_data.element_ref_index + 1 > ref_count) {
		ref_count = bind_data.element_ref_index + 1;
	}
	vector<unique_ptr<BaseStatistics>> ref_stats(ref_count);

	// seed the captured columns' statistics at their body reference indices
	for (idx_t i = 0; i < bind_data.capture_count; i++) {
		ref_stats[capture_ref_base + i] = child_stats[base_child + i].ToUnique();
	}

	// seed the list-element parameter's statistics from the list argument's child statistics
	if (bind_data.element_ref_index != DConstants::INVALID_INDEX && bind_data.element_ref_index < ref_stats.size() &&
	    child_stats[0].GetStatsType() == StatisticsType::LIST_STATS) {
		ref_stats[bind_data.element_ref_index] = ListStats::GetChildStats(child_stats[0]).ToUnique();
	}

	// inside the lambda body a captured value is evaluated once per list element, so its (cardinality-dependent)
	// total string length no longer holds; clear it to avoid seeding an understated total into the body
	for (auto &ref_stat : ref_stats) {
		if (ref_stat) {
			ref_stat->ResetTotalStringLength();
		}
	}

	input.propagator->PropagateLambdaStatistics(bind_data.lambda_expr, ref_stats);
	// we do not derive a result statistic for the lambda function itself
	return nullptr;
}

void LambdaFunctions::ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteLambda<ListTransformFunctor>(args, state, result);
}

void LambdaFunctions::ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteLambda<ListFilterFunctor>(args, state, result);
}

} // namespace duckdb
