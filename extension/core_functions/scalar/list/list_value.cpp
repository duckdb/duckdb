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

struct AssignInfo {
	explicit AssignInfo(DataChunk &args, const optional_ptr<vector<idx_t>> col_offsets = nullptr) :
		col_offsets(col_offsets) {
	}

	const optional_ptr<vector<idx_t>> col_offsets;
};

struct PrimitiveAssign {
	template<class T>
	static T Assign(const T &input, Vector &result, AssignInfo &info, const idx_t col) {
		return input;
	}
};

struct StringAssign {
	template <class T>
	static T Assign(const T &input, Vector &result, AssignInfo &info, const idx_t col) {
		return StringVector::AddStringOrBlob(result, input);
	}
};

struct ListEntryAssign {
	template <class T>
	static T Assign(const T &input, Vector &result, AssignInfo &info, const idx_t col) {
		const auto length = input.length;
		const auto offset = (*info.col_offsets)[col] + input.offset;
		return list_entry_t(offset, length);
	}
};

template<class T, class OP = PrimitiveAssign>
static void TemplatedPopulateChild(DataChunk &args, Vector &result, const optional_ptr<vector<idx_t>> col_offsets = nullptr) {
	auto info = AssignInfo(args, col_offsets);
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
			auto val = OP::template Assign<T>(input_data[input_idx], result, info, col);
			result_data[result_idx] = val;
		}
	}
}

template<class OP = PrimitiveAssign>
static void PopulateChild(DataChunk &args, Vector &result, const optional_ptr<vector<idx_t>> col_offsets = nullptr) {
	switch (result.GetType().InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			return TemplatedPopulateChild<int8_t>(args, result);
		case PhysicalType::INT16:
			return TemplatedPopulateChild<int16_t>(args, result);
		case PhysicalType::INT32:
			TemplatedPopulateChild<int32_t>(args, result);
			// TODO: nit: return
			break;
		case PhysicalType::INT64:
			TemplatedPopulateChild<int64_t>(args, result);
			break;
		case PhysicalType::UINT8:
			TemplatedPopulateChild<uint8_t>(args, result);
			break;
		case PhysicalType::UINT16:
			TemplatedPopulateChild<uint16_t>(args, result);
			break;
		case PhysicalType::UINT32:
			TemplatedPopulateChild<uint32_t>(args, result);
			break;
		case PhysicalType::UINT64:
			TemplatedPopulateChild<uint64_t>(args, result);
			break;
		case PhysicalType::INT128:
			TemplatedPopulateChild<hugeint_t>(args, result);
			break;
		case PhysicalType::UINT128:
			TemplatedPopulateChild<uhugeint_t>(args, result);
			break;
		case PhysicalType::FLOAT:
			TemplatedPopulateChild<float>(args, result);
			break;
		case PhysicalType::DOUBLE:
			TemplatedPopulateChild<double>(args, result);
			break;
		case PhysicalType::INTERVAL:
			TemplatedPopulateChild<interval_t>(args, result);
			break;
		case PhysicalType::VARCHAR:
			TemplatedPopulateChild<string_t, StringAssign>(args, result);
			break;
		case PhysicalType::LIST:
			TemplatedPopulateChild<list_entry_t, ListEntryAssign>(args, result, col_offsets);
			break;
		default: {
			// TODO: Throw internal error.
			break;
		}
	}
	// UNKNOWN and INVALID should throw
	// Check ARRAY ? Check BIT ?
}

static void PrimitiveFunction(DataChunk &args, Vector &result) {
	ListVector::Reserve(result, args.size() * args.ColumnCount());
	auto &result_child = ListVector::GetEntry(result);
	PopulateChild(args, result_child);
}

static void ListFunction(DataChunk &args, Vector &result) {
	const idx_t column_count = args.ColumnCount();
	ListVector::Reserve(result, args.size() * column_count);

	vector<idx_t> col_offsets;
	idx_t offset_sum = 0;
	for (idx_t col = 0; col < column_count; col++) {
		col_offsets.push_back(offset_sum);
		auto &list = args.data[col];
		const auto length = ListVector::GetListSize(list);
		offset_sum += length;
	}

	auto &result_child = ListVector::GetEntry(result);
	ListVector::Reserve(result_child, offset_sum);

	// The result vector is [[[a], [b]], ...], result_nested_child is [a, b, ...].
	auto &result_nested_child = ListVector::GetEntry(result_child);
	vector<LogicalType> types;
	for (idx_t col = 0; col < column_count; col++) {
		auto list = args.data[col];
		types.push_back(list.GetType());

		const auto length = ListVector::GetListSize(list);
		if (length == 0) {
			continue;
		}
		auto &child_vector = ListVector::GetEntry(list);
		VectorOperations::Copy(child_vector, result_nested_child, length, 0, col_offsets[col]);
	}

	DataChunk chunk;
	chunk.InitializeEmpty(types);
	chunk.SetCardinality(args.size());

	for (idx_t col = 0; col < column_count; col++) {
		auto list = args.data[col];
		chunk.data[col].Reference(list);
	}
	PopulateChild(chunk, result_child, col_offsets);
	ListVector::SetListSize(result_child, offset_sum);
}

static void StructFunction(DataChunk &args, Vector &result) {
	const idx_t column_count = args.ColumnCount();
	ListVector::Reserve(result, column_count * args.size());

	auto &result_child = ListVector::GetEntry(result);
	auto &result_child_members = StructVector::GetEntries(result_child);

	for (idx_t member_idx = 0; member_idx < result_child_members.size(); member_idx++) {
		// Same type for each column's member.
		vector<LogicalType> types;
		const auto member_type = result_child_members[member_idx]->GetType();
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

		switch (result_child_members[member_idx]->GetType().InternalType()) {
			case PhysicalType::LIST:
				ListFunction(chunk, *result_child_members[member_idx]);
				break;
			case PhysicalType::STRUCT:
				StructFunction(chunk, *result_child_members[member_idx]);
				break;
			default: {
				PopulateChild(chunk, *result_child_members[member_idx]);
				break;
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

	auto &result_type = ListVector::GetEntry(result).GetType();
	switch (result_type.InternalType()) {
		case PhysicalType::LIST:
			ListFunction(args, result);
			break;
		case PhysicalType::STRUCT:
			StructFunction(args, result);
			break;
		default: {
			PrimitiveFunction(args, result);
			break;
		}
	}

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
