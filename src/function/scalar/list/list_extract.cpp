#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"

namespace duckdb {

static optional_idx TryGetChildOffset(const list_entry_t &list_entry, const int64_t offset) {
	// 1-based indexing
	if (offset == 0) {
		return optional_idx::Invalid();
	}

	const auto index_offset = (offset > 0) ? offset - 1 : offset;
	if (index_offset < 0) {
		const auto signed_list_length = UnsafeNumericCast<int64_t>(list_entry.length);
		if (signed_list_length + index_offset < 0) {
			return optional_idx::Invalid();
		}
		return optional_idx(list_entry.offset + UnsafeNumericCast<idx_t>(signed_list_length + index_offset));
	}

	const auto unsigned_offset = UnsafeNumericCast<idx_t>(index_offset);

	// Check that the offset is within the list
	if (unsigned_offset >= list_entry.length) {
		return optional_idx::Invalid();
	}

	return optional_idx(list_entry.offset + unsigned_offset);
}

static void ExecuteListExtract(Vector &result, Vector &list, Vector &offsets, const idx_t count) {
	D_ASSERT(list.GetType().id() == LogicalTypeId::LIST);
	UnifiedVectorFormat list_data;
	UnifiedVectorFormat offsets_data;

	list.ToUnifiedFormat(count, list_data);
	offsets.ToUnifiedFormat(count, offsets_data);

	const auto list_ptr = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto offsets_ptr = UnifiedVectorFormat::GetData<int64_t>(offsets_data);

	UnifiedVectorFormat child_data;
	auto &child_vector = ListVector::GetEntry(list);
	auto child_count = ListVector::GetListSize(list);
	child_vector.ToUnifiedFormat(child_count, child_data);

	SelectionVector sel(count);
	vector<idx_t> invalid_offsets;

	optional_idx first_valid_child_idx;
	for (idx_t i = 0; i < count; i++) {
		const auto list_index = list_data.sel->get_index(i);
		const auto offsets_index = offsets_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_index) || !offsets_data.validity.RowIsValid(offsets_index)) {
			invalid_offsets.push_back(i);
			continue;
		}

		const auto child_offset = TryGetChildOffset(list_ptr[list_index], offsets_ptr[offsets_index]);

		if (!child_offset.IsValid()) {
			invalid_offsets.push_back(i);
			continue;
		}

		const auto child_idx = child_data.sel->get_index(child_offset.GetIndex());
		sel.set_index(i, child_idx);

		if (!first_valid_child_idx.IsValid()) {
			// Save the first valid child as a dummy index to copy in VectorOperations::Copy later
			first_valid_child_idx = child_idx;
		}
	}

	if (first_valid_child_idx.IsValid()) {
		// Only copy if we found at least one valid child
		for (const auto &invalid_offset : invalid_offsets) {
			sel.set_index(invalid_offset, first_valid_child_idx.GetIndex());
		}
		VectorOperations::Copy(child_vector, result, sel, count, 0, 0);
	}

	// Copy:ing the vectors also copies the validity mask, so we set the rows with invalid offsets (0) to false here.
	for (const auto &invalid_idx : invalid_offsets) {
		FlatVector::SetNull(result, invalid_idx, true);
	}

	if (count == 1 || (list.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	                   offsets.GetVectorType() == VectorType::CONSTANT_VECTOR)) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(count);
}

static void ExecuteStringExtract(Vector &result, Vector &input_vector, Vector &subscript_vector, const idx_t count) {
	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    input_vector, subscript_vector, result, count,
	    [&](string_t input_string, int64_t subscript) { return SubstringUnicode(result, input_string, subscript, 1); });
}

static void ListExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &base = args.data[0];
	Vector &subscript = args.data[1];

	switch (base.GetType().id()) {
	case LogicalTypeId::LIST:
		ExecuteListExtract(result, base, subscript, count);
		break;
	case LogicalTypeId::VARCHAR:
		ExecuteStringExtract(result, base, subscript, count);
		break;
	case LogicalTypeId::SQLNULL:
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static unique_ptr<FunctionData> ListExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());
	// list extract returns the child type of the list as return type
	auto child_type = ListType::GetChildType(arguments[0]->return_type);

	bound_function.return_type = child_type;
	bound_function.arguments[0] = LogicalType::LIST(child_type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListExtractStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &list_child_stats = ListStats::GetChildStats(child_stats[0]);
	auto child_copy = list_child_stats.Copy();
	// list_extract always pushes a NULL, since if the offset is out of range for a list it inserts a null
	child_copy.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	return child_copy.ToUnique();
}

ScalarFunctionSet ListExtractFun::GetFunctions() {
	ScalarFunctionSet list_extract_set("list_extract");

	// the arguments and return types are actually set in the binder function
	ScalarFunction lfun({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT}, LogicalType::ANY,
	                    ListExtractFunction, ListExtractBind, nullptr, ListExtractStats);

	ScalarFunction sfun({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR, ListExtractFunction);

	list_extract_set.AddFunction(lfun);
	list_extract_set.AddFunction(sfun);
	return list_extract_set;
}

ScalarFunctionSet ArrayExtractFun::GetFunctions() {
	ScalarFunctionSet array_extract_set("array_extract");

	// the arguments and return types are actually set in the binder function
	ScalarFunction lfun({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT}, LogicalType::ANY,
	                    ListExtractFunction, ListExtractBind, nullptr, ListExtractStats);

	ScalarFunction sfun({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR, ListExtractFunction);

	array_extract_set.AddFunction(lfun);
	array_extract_set.AddFunction(sfun);
	array_extract_set.AddFunction(GetKeyExtractFunction());
	array_extract_set.AddFunction(GetIndexExtractFunction());
	return array_extract_set;
}

} // namespace duckdb
