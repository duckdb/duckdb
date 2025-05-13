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

template<class T>
struct ListValueInfo {
	ListValueInfo(DataChunk &args, Vector &result, const optional_ptr<vector<idx_t>> col_offsets = nullptr) :
	list_size(args.ColumnCount()), row_count(args.size()), col_offsets(col_offsets) {
		result_list_child = &ListVector::GetEntry(result);
		result_child_data = FlatVector::GetData<T>(*result_list_child);
		result_child_validity = &FlatVector::Validity(*result_list_child);

		unified_format = args.ToUnifiedFormat();
	}

	const idx_t list_size;
	const idx_t row_count;

	Vector *result_list_child;
	T *result_child_data;
	ValidityMask *result_child_validity;

	unsafe_unique_array<UnifiedVectorFormat> unified_format;

	optional_ptr<vector<idx_t>> col_offsets;
};

struct ListValueAssign {
	template<class T>
	static T Assign(Vector &result, ListValueInfo<T> &info, idx_t input_idx, idx_t col) {
		return *UnifiedVectorFormat::GetData<T>(info.unified_format[col]);
	}
};

struct ListValueStringAssign {
	template <class T>
	static T Assign(Vector &result, ListValueInfo<T> &info, idx_t input_idx, idx_t col) {
		auto input_data = *UnifiedVectorFormat::GetData<T>(info.unified_format[col]);
		return StringVector::AddStringOrBlob(result, input_data);
	}
};

struct ListValueListEntryAssign {
	template <class T>
	static T Assign(Vector &result, ListValueInfo<T> &info, idx_t input_idx, idx_t col) {
		const auto input_data = UnifiedVectorFormat::GetData<list_entry_t>(info.unified_format[col]);
		const auto length = input_data[input_idx].length;
		const auto offset = (*info.col_offsets)[col] + input_data[input_idx].offset;
		return list_entry_t(offset, length);
	}
};

template<class T, class OP = ListValueAssign>
static void TemplatedPopulateList(DataChunk &args, Vector &result, const optional_ptr<vector<idx_t>> col_offsets = nullptr) {
	auto info = ListValueInfo<T>(args, result, col_offsets);

	for (idx_t row = 0; row < info.row_count; row++) {
		for (idx_t col = 0; col < info.list_size; col++) {
			auto input_idx = info.unified_format[col].sel->get_index(row);
			auto result_idx = row * info.list_size + col;
			if (info.unified_format[col].validity.RowIsValid(input_idx)) {
				info.result_child_data[result_idx] = OP::template Assign<T>(*info.result_list_child, info, input_idx, col);
			} else {
				info.result_child_validity->SetInvalid(result_idx);
			}
		}
	}

}

static void TemplatedListValueFunctionFallback(DataChunk &args, Vector &result) {
	auto &child_type = ListType::GetChildType(result.GetType());
	const auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).DefaultCastAs(child_type);
			ListVector::PushBack(result, val);
		}
		result_data[i].length = args.ColumnCount();
	}
}

template<class OP = ListValueAssign>
static void PopulateList(DataChunk &args, Vector &result, const PhysicalType child_type, const optional_ptr<vector<idx_t>> col_offsets = nullptr) {
	switch (child_type) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedPopulateList<int8_t>(args, result);
			break;
		case PhysicalType::INT16:
			TemplatedPopulateList<int16_t>(args, result);
			break;
		case PhysicalType::INT32:
			TemplatedPopulateList<int32_t>(args, result);
			break;
		case PhysicalType::INT64:
			TemplatedPopulateList<int64_t>(args, result);
			break;
		case PhysicalType::UINT8:
			TemplatedPopulateList<uint8_t>(args, result);
			break;
		case PhysicalType::UINT16:
			TemplatedPopulateList<uint16_t>(args, result);
			break;
		case PhysicalType::UINT32:
			TemplatedPopulateList<uint32_t>(args, result);
			break;
		case PhysicalType::UINT64:
			TemplatedPopulateList<uint64_t>(args, result);
			break;
		case PhysicalType::INT128:
			TemplatedPopulateList<hugeint_t>(args, result);
			break;
		case PhysicalType::UINT128:
			TemplatedPopulateList<uhugeint_t>(args, result);
			break;
		case PhysicalType::FLOAT:
			TemplatedPopulateList<float>(args, result);
			break;
		case PhysicalType::DOUBLE:
			TemplatedPopulateList<double>(args, result);
			break;
		case PhysicalType::INTERVAL:
			TemplatedPopulateList<interval_t>(args, result);
			break;
		case PhysicalType::VARCHAR:
			TemplatedPopulateList<string_t, ListValueStringAssign>(args, result);
			break;
		case PhysicalType::LIST:
			TemplatedPopulateList<list_entry_t, ListValueListEntryAssign>(args, result, col_offsets);
			break;
		default: {
			TemplatedListValueFunctionFallback(args, result);
			break;
		}
	}
}

template <class OP = ListValueAssign>
static void TemplatedListValueFunction(DataChunk &args, Vector &result) {
	const auto list_size = args.ColumnCount();
	ListVector::Reserve(result, args.size() * list_size);

	auto &result_type = ListVector::GetEntry(result).GetType();
	PopulateList<OP>(args, result, result_type.InternalType());

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		result_data[row].offset = row * list_size;
		result_data[row].length = list_size;
	}
	ListVector::SetListSize(result, args.size() * list_size);
}

static void ListValueListFunction(DataChunk &args, Vector &result) {
	const idx_t list_size = args.ColumnCount();
	ListVector::Reserve(result, args.size() * list_size);

	vector<idx_t> col_offsets;
	idx_t offset_sum = 0;
	for (idx_t i = 0; i < list_size; i++) {
		col_offsets.push_back(offset_sum);
		auto &list = args.data[i];
		const auto length = ListVector::GetListSize(list);
		offset_sum += length;
	}

	auto &result_list = ListVector::GetEntry(result);
	ListVector::Reserve(result_list, offset_sum);

	auto &result_child_vector = ListVector::GetEntry(result_list);
	for (idx_t i = 0; i < list_size; i++) {
		auto list = args.data[i];
		const auto length = ListVector::GetListSize(list);
		if (length == 0) {
			continue;
		}
		auto &child_vector = ListVector::GetEntry(list);
		VectorOperations::Copy(child_vector, result_child_vector, length, 0, col_offsets[i]);
	}

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		result_data[row].offset = row * list_size;
		result_data[row].length = list_size;
	}
	ListVector::SetListSize(result_list, offset_sum);
}

static void ListValueStructFunction(DataChunk &args, Vector &result) {
	const idx_t list_size = args.ColumnCount();
	const idx_t rows = args.size();
	const idx_t result_child_size = list_size * rows;

	ListVector::Reserve(result, result_child_size);

	// The length of the child vector is the number of structs * the number
	auto &result_list = ListVector::GetEntry(result);
	auto &result_entries = StructVector::GetEntries(result_list);

	const auto &member_vector = args.data[0];
	auto &struct_members = StructVector::GetEntries(member_vector);
	for (idx_t member_idx = 0; member_idx < result_entries.size(); member_idx++) {
		// types vector holds one type of the struct member
		vector<LogicalType> types;
		auto member_type = struct_members[member_idx]->GetType();
		for (idx_t col = 0; col < list_size; col++) {
			types.push_back(member_type);
		}

		DataChunk member_chunk;
		member_chunk.InitializeEmpty(types);
		member_chunk.SetCardinality(rows);

		for (idx_t col_idx = 0; col_idx < list_size; col_idx++) {
			const auto &struct_vector = args.data[col_idx];
			auto &members = StructVector::GetEntries(struct_vector);

			member_chunk.data[col_idx].Reference(*members[member_idx]);
		}

		PopulateList(member_chunk, *result_entries[member_idx], types[member_idx].InternalType());
	}

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		result_data[row].offset = row * list_size;
		result_data[row].length = list_size;
	}

	// const auto result_data = FlatVector::GetData<list_entry_t>(result);
	// auto &result_list_validity = FlatVector::Validity(result_list);
	//
	// const auto args_unified_format = args.ToUnifiedFormat();
	//
	// SelectionVector sel(result_child_size);
	// for (idx_t r = 0; r < rows; r++) {
	// 	for (idx_t c = 0; c < list_size; c++) {
	// 		const auto result_idx = r * list_size + c;
	// 		sel.set_index(result_idx, c * rows + r);
	//
	// 		const auto input_idx = args_unified_format[c].sel->get_index(r);
	// 		if (!args_unified_format[c].validity.RowIsValid(input_idx)) {
	// 			result_list_validity.SetInvalid(result_idx);
	// 		}
	// 	}
	// 	result_data[r].offset = r * list_size;
	// 	result_data[r].length = list_size;
	// }
	//
	// for (idx_t c = 0; c < result_entries.size(); c++) {
	// 	result_entries[c]->Slice(sel, result_child_size);
	// 	result_entries[c]->Flatten(result_child_size);
	// }

	ListVector::SetListSize(result, result_child_size);
}

static void ListValueDetermineFunction(DataChunk &args, Vector &result) {
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto &result_type = ListVector::GetEntry(result).GetType();
	switch (result_type.InternalType()) {
		case PhysicalType::LIST:
			ListValueListFunction(args, result);
			break;
		case PhysicalType::STRUCT:
			ListValueStructFunction(args, result);
			break;
		default: {
			TemplatedListValueFunction(args, result);
			break;
		}
	}
}

static void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	if (args.ColumnCount() == 0) {
		// no columns - early out - result is a constant empty list
		auto result_data = FlatVector::GetData<list_entry_t>(result);
		result_data[0].length = 0;
		result_data[0].offset = 0;
		return;
	}

	ListValueDetermineFunction(args, result);
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
