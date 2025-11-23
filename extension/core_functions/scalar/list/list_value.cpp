#include "core_functions/scalar/list_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

namespace duckdb {

namespace {

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
void TemplatedPopulateChild(DataChunk &args, Vector &result) {
	const auto column_count = args.ColumnCount();
	const auto row_count = args.size();

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

void PopulateChildFallback(DataChunk &args, Vector &result) {
	auto &child_type = ListType::GetChildType(result.GetType());
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).DefaultCastAs(child_type);
			ListVector::PushBack(result, val);
		}
		result_data[i].length = args.ColumnCount();
	}
}

void ListFunction(DataChunk &args, Vector &result);
bool StructFunction(DataChunk &args, Vector &result);

template <class OP = PrimitiveAssign>
bool PopulateChild(DataChunk &args, Vector &result) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedPopulateChild<int8_t>(args, result);
		break;
	case PhysicalType::INT16:
		TemplatedPopulateChild<int16_t>(args, result);
		break;
	case PhysicalType::INT32:
		TemplatedPopulateChild<int32_t>(args, result);
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
		ListFunction(args, result);
		break;
	case PhysicalType::STRUCT:
		return StructFunction(args, result);
	case PhysicalType::UNKNOWN:
	case PhysicalType::INVALID:
		throw InternalException("Cannot create a list of types %s - an explicit cast is required",
		                        result.GetType().ToString());
	default:
		// execute the fallback instead, which requires the parent result
		return false;
	}
	return true;
}

void ListFunction(DataChunk &args, Vector &result) {
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
			const auto input_idx = unified_format[col].sel->get_index(row);
			const auto result_idx = row * column_count + col;
			if (!unified_format[col].validity.RowIsValid(input_idx)) {
				result_validity->SetInvalid(result_idx);
				continue;
			}
			const auto input = col_data[col][input_idx];
			const auto length = input.length;
			const auto offset = col_offsets[col] + input.offset;
			result_data[result_idx] = list_entry_t(offset, length);
		}
	}
	ListVector::SetListSize(result, offset_sum);
}

bool StructFunction(DataChunk &args, Vector &result) {
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

		if (!PopulateChild(chunk, *result_members[member_idx])) {
			return false;
		}
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

	return true;
}

void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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

	if (!PopulateChild(args, result_child)) {
		PopulateChildFallback(args, result);
	}

	const idx_t column_count = args.ColumnCount();
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		result_data[row].offset = row * column_count;
		result_data[row].length = column_count;
	}
	ListVector::SetListSize(result, column_count * args.size());
}

unique_ptr<FunctionData> UnpivotBind(ClientContext &context, ScalarFunction &bound_function,
                                     vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	LogicalType child_type =
	    arguments.empty() ? LogicalType::SQLNULL : ExpressionBinder::GetExpressionReturnType(*arguments[0]);
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto arg_type = ExpressionBinder::GetExpressionReturnType(*arguments[i]);
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, arg_type, child_type)) {
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
			auto error = StringUtil::Format("Cannot unpivot columns of types %s and %s - an explicit cast is required",
			                                child_type.ToString(), arg_type.ToString());
			throw BinderException(arguments[i]->GetQueryLocation(),
			                      QueryErrorContext::Format(list_arguments, error, error_index, false));
		}
	}
	child_type = LogicalType::NormalizeType(child_type);

	// this is more for completeness reasons
	bound_function.varargs = child_type;
	bound_function.SetReturnType(LogicalType::LIST(child_type));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
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

} // namespace

ScalarFunctionSet ListValueFun::GetFunctions() {
	ScalarFunctionSet set("list_value");

	// Overload for 0 arguments, which returns an empty list.
	ScalarFunction empty_fun({}, LogicalType::LIST(LogicalType::SQLNULL), ListValueFunction, nullptr, nullptr,
	                         ListValueStats);
	set.AddFunction(empty_fun);

	// Overload for 1 + N arguments, which returns a list of the arguments.
	auto element_type = LogicalType::TEMPLATE("T");
	ScalarFunction value_fun({element_type}, LogicalType::LIST(element_type), ListValueFunction, nullptr, nullptr,
	                         ListValueStats);
	value_fun.varargs = element_type;
	value_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	set.AddFunction(value_fun);

	return set;
}

ScalarFunction UnpivotListFun::GetFunction() {
	ScalarFunction fun("unpivot_list", {}, LogicalTypeId::LIST, ListValueFunction, UnpivotBind, nullptr,
	                   ListValueStats);
	fun.varargs = LogicalTypeId::ANY;
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
