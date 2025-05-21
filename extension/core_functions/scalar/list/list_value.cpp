#include "core_functions/scalar/list_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

struct PrimitiveAssign {
	template <class T>
	static T Assign(const T &input, Vector &result) {
		return input;
	}
};

struct StringAssign {
	template <class T>
	static T Assign(const T &input, Vector &result) {
		return StringVector::AddStringOrBlob(result, input);
	}
};

template <class T, class OP = PrimitiveAssign>
static void TemplatedPopulateChild(DataChunk &args, Vector &result) {
	auto column_count = args.ColumnCount();
	auto row_count = args.size();

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto result_validity = &FlatVector::Validity(result);

	auto unified_format = args.ToUnifiedFormat();
	for (idx_t row = 0; row < row_count; row++) {
		for (idx_t col = 0; col < column_count; col++) {
			auto input_idx = unified_format[col].sel->get_index(row);
			auto result_idx = row * column_count + col;
			if (!unified_format[col].validity.RowIsValid(input_idx)) {
				result_validity->SetInvalid(result_idx);
				continue;
			}
			const auto input_data = UnifiedVectorFormat::GetData<T>(unified_format[col]);
			auto val = OP::template Assign<T>(input_data[input_idx], result);
			result_data[result_idx] = val;
		}
	}
}

static void ListFunction(DataChunk &args, Vector &result);
static void ArrayFunction(DataChunk &args, Vector &result);
static void StructFunction(DataChunk &args, Vector &result);

template <class OP = PrimitiveAssign>
static void PopulateChild(DataChunk &args, Vector &result) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedPopulateChild<int8_t>(args, result);
	case PhysicalType::INT16:
		return TemplatedPopulateChild<int16_t>(args, result);
	case PhysicalType::INT32:
		return TemplatedPopulateChild<int32_t>(args, result);
	case PhysicalType::INT64:
		return TemplatedPopulateChild<int64_t>(args, result);
	case PhysicalType::UINT8:
		return TemplatedPopulateChild<uint8_t>(args, result);
	case PhysicalType::UINT16:
		return TemplatedPopulateChild<uint16_t>(args, result);
	case PhysicalType::UINT32:
		return TemplatedPopulateChild<uint32_t>(args, result);
	case PhysicalType::UINT64:
		return TemplatedPopulateChild<uint64_t>(args, result);
	case PhysicalType::INT128:
		return TemplatedPopulateChild<hugeint_t>(args, result);
	case PhysicalType::UINT128:
		return TemplatedPopulateChild<uhugeint_t>(args, result);
	case PhysicalType::FLOAT:
		return TemplatedPopulateChild<float>(args, result);
	case PhysicalType::DOUBLE:
		return TemplatedPopulateChild<double>(args, result);
	case PhysicalType::INTERVAL:
		return TemplatedPopulateChild<interval_t>(args, result);
	case PhysicalType::VARCHAR:
		return TemplatedPopulateChild<string_t, StringAssign>(args, result);
	case PhysicalType::LIST:
		return ListFunction(args, result);
	case PhysicalType::ARRAY:
		return ArrayFunction(args, result);
	case PhysicalType::STRUCT:
		return StructFunction(args, result);
	default: {
		throw InternalException("TODO");
	}
	}
	// UNKNOWN and INVALID should throw
	// Check ARRAY ? Check BIT ?
}

static void ListFunction(DataChunk &args, Vector &result) {
	const idx_t column_count = args.ColumnCount();

	vector<idx_t> col_offsets;
	idx_t offset_sum = 0;
	for (idx_t col = 0; col < column_count; col++) {
		col_offsets.push_back(offset_sum);
		auto &list = args.data[col];
		const auto length = ListVector::GetListSize(list);
		offset_sum += length;
	}

	ListVector::Reserve(result, offset_sum);

	// The result vector is [[a], [b], ...], result_child is [a, b, ...].
	auto &result_child = ListVector::GetEntry(result);

	auto unified_format = args.ToUnifiedFormat();
	vector<const list_entry_t *> col_data;
	for (idx_t col = 0; col < column_count; col++) {
		auto list = args.data[col];
		col_data.push_back(UnifiedVectorFormat::GetData<list_entry_t>(unified_format[col]));

		const auto length = ListVector::GetListSize(list);
		if (length == 0) {
			continue;
		}
		auto &child_vector = ListVector::GetEntry(list);
		VectorOperations::Copy(child_vector, result_child, length, 0, col_offsets[col]);
	}

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto result_validity = &FlatVector::Validity(result);

	for (idx_t row = 0; row < args.size(); row++) {
		for (idx_t col = 0; col < column_count; col++) {
			auto input_idx = unified_format[col].sel->get_index(row);
			auto result_idx = row * column_count + col;
			if (!unified_format[col].validity.RowIsValid(input_idx)) {
				result_validity->SetInvalid(result_idx);
				continue;
			}
			auto input = col_data[col][input_idx];
			const auto length = input.length;
			const auto offset = col_offsets[col] + input.offset;
			result_data[result_idx] = list_entry_t(offset, length);
		}
	}
	ListVector::SetListSize(result, offset_sum);
}

static void ArrayFunction(DataChunk &args, Vector &result) {
	const idx_t column_count = args.ColumnCount();
	;
	const auto array_total_size = ArrayType::GetSize(args.data[0].GetType()) * args.size();

	// The result vector is [[a], [b], ...], result_child is [a, b, ...].
	auto &result_child = ArrayVector::GetEntry(result);

	auto unified_format = args.ToUnifiedFormat();
	for (idx_t col = 0; col < column_count; col++) {
		auto &array = args.data[col];
		auto &child_vector = ArrayVector::GetEntry(array);
		VectorOperations::Copy(child_vector, result_child, array_total_size, 0, col * array_total_size);
	}

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto result_validity = &FlatVector::Validity(result);

	for (idx_t row = 0; row < args.size(); row++) {
		for (idx_t col = 0; col < column_count; col++) {
			auto input_idx = unified_format[col].sel->get_index(row);
			auto result_idx = row * column_count + col;
			if (!unified_format[col].validity.RowIsValid(input_idx)) {
				result_validity->SetInvalid(result_idx);
			}
		}
	}
}

static void StructFunction(DataChunk &args, Vector &result) {
	const idx_t column_count = args.ColumnCount();
	auto &result_members = StructVector::GetEntries(result);

	for (idx_t member_idx = 0; member_idx < result_members.size(); member_idx++) {
		// Same type for each column's member.
		vector<LogicalType> types;
		const auto member_type = result_members[member_idx]->GetType();
		for (idx_t col = 0; col < column_count; col++) {
			types.push_back(member_type);
		}

		DataChunk chunk;
		chunk.InitializeEmpty(types);
		chunk.SetCardinality(args.size());

		for (idx_t col = 0; col < column_count; col++) {
			const auto &struct_vector = args.data[col];
			auto &struct_vector_members = StructVector::GetEntries(struct_vector);
			chunk.data[col].Reference(*struct_vector_members[member_idx]);
		}

		PopulateChild(chunk, *result_members[member_idx]);
	}

	// Set the top level result validity
	const auto unified_format = args.ToUnifiedFormat();
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t row = 0; row < args.size(); row++) {
		for (idx_t col = 0; col < column_count; col++) {
			const auto input_idx = unified_format[col].sel->get_index(row);
			const auto result_idx = row * column_count + col;
			if (!unified_format[col].validity.RowIsValid(input_idx)) {
				result_validity.SetInvalid(result_idx);
			}
		}
	}

	// Set the top level result validity
	const auto unified_format = args.ToUnifiedFormat();
	auto &result_validity = FlatVector::Validity(result_child);
	for (idx_t row = 0; row < args.size(); row++) {
		for (idx_t col = 0; col < column_count; col++) {
			const auto input_idx = unified_format[col].sel->get_index(row);
			const auto result_idx = row * column_count + col;
			if (!unified_format[col].validity.RowIsValid(input_idx)) {
				result_validity.SetInvalid(result_idx);
			}
		}
	}
}

static void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	if (args.ColumnCount() == 0) {
		// Early out because the result is a constant empty list.
		auto result_data = FlatVector::GetData<list_entry_t>(result);
		result_data[0].length = 0;
		result_data[0].offset = 0;
		return;
	}

	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	ListVector::Reserve(result, args.size() * args.ColumnCount());
	auto &result_child = ListVector::GetEntry(result);
	PopulateChild(args, result_child);

	const idx_t column_count = args.ColumnCount();
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		result_data[row].offset = row * column_count;
		result_data[row].length = column_count;
	}
	ListVector::SetListSize(result, column_count * args.size());
}

template <bool IS_UNPIVOT = false>
static unique_ptr<FunctionData> ListValueBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	LogicalType child_type =
	    arguments.empty() ? LogicalType::SQLNULL : ExpressionBinder::GetExpressionReturnType(*arguments[0]);
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto arg_type = ExpressionBinder::GetExpressionReturnType(*arguments[i]);
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, arg_type, child_type)) {
			if (IS_UNPIVOT) {
				string list_arguments = "Full list: ";
				idx_t error_index = list_arguments.size();
				for (idx_t k = 0; k < arguments.size(); k++) {
					if (k > 0) {
						list_arguments += ", ";
					}
					if (k == i) {
						error_index = list_arguments.size();
					}
					list_arguments += arguments[k]->ToString() + " " + arguments[k]->return_type.ToString();
				}
				auto error =
				    StringUtil::Format("Cannot unpivot columns of types %s and %s - an explicit cast is required",
				                       child_type.ToString(), arg_type.ToString());
				throw BinderException(arguments[i]->GetQueryLocation(),
				                      QueryErrorContext::Format(list_arguments, error, error_index, false));
			} else {
				throw BinderException(arguments[i]->GetQueryLocation(),
				                      "Cannot create a list of types %s and %s - an explicit cast is required",
				                      child_type.ToString(), arg_type.ToString());
			}
		}
	}
	child_type = LogicalType::NormalizeType(child_type);

	// this is more for completeness reasons
	bound_function.varargs = child_type;
	bound_function.return_type = LogicalType::LIST(child_type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> ListValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ListStats::CreateEmpty(expr.return_type);
	auto &list_child_stats = ListStats::GetChildStats(list_stats);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		list_child_stats.Merge(child_stats[i]);
	}
	return list_stats.ToUnique();
}

ScalarFunction ListValueFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("list_value", {}, LogicalTypeId::LIST, ListValueFunction, ListValueBind, nullptr,
	                   ListValueStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

ScalarFunction UnpivotListFun::GetFunction() {
	auto fun = ListValueFun::GetFunction();
	fun.name = "unpivot_list";
	fun.bind = ListValueBind<true>;
	return fun;
}

} // namespace duckdb
