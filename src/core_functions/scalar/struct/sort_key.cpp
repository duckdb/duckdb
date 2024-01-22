#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/core_functions/scalar/struct_functions.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void SortKeyExecute(DataChunk &sort, ExpressionState &state, Vector &result) {
	//	Make sure we have the correct type
	const auto &return_type = result.GetType();
	D_ASSERT(return_type.id() != LogicalTypeId::SORT_KEY);

	//	Extract the sort keys
	auto &info = return_type.AuxInfo()->Cast<SortKeyTypeInfo>();
	D_ASSERT(info.order_bys.size() == sort.data.size());

	vector<BoundOrderByNode> sorts;
	sorts.reserve(info.order_bys.size());
	for (const auto &order_by : info.order_bys) {
		sorts.emplace_back(BoundOrderByNode(order_by.type, order_by.null_order, nullptr));
	}
	SortLayout sort_layout(sorts);

	// Build and serialize sorting data to radix sortable rows
	Vector addresses(LogicalType::POINTER);
	const SelectionVector &sel_ptr = *FlatVector::IncrementalSelectionVector();
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	auto base_ptr = FlatVector::GetData<data_t>(result);
	for (idx_t i = 0; i < sort.size(); ++i) {
		data_pointers[i] = base_ptr;
		base_ptr += sort_layout.entry_size;
	}

	for (column_t sort_col = 0; sort_col < sort.ColumnCount(); ++sort_col) {
		bool has_null = sort_layout.has_null[sort_col];
		bool nulls_first = sort_layout.order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sort_layout.order_types[sort_col] == OrderType::DESCENDING;
		RowOperations::RadixScatter(sort.data[sort_col], sort.size(), sel_ptr, sort.size(), data_pointers, desc,
		                            has_null, nulls_first, sort_layout.prefix_lengths[sort_col],
		                            sort_layout.column_sizes[sort_col]);
	}

	// Also fully serialize blob sorting columns (to be able to break ties)
	if (sort_layout.all_constant) {
		return;
	}

	throw NotImplementedException("Non-constant sort keys are not supported yet");
#if 0
	//	For each row allocate a blob and point the variable size data into it.
	DataChunk blob_chunk;
	blob_chunk.SetCardinality(sort.size());
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		if (!sort_layout.constant_size[sort_col]) {
			blob_chunk.data.emplace_back(sort.data[sort_col]);
		}
	}


	handles = blob_sorting_data->Build(blob_chunk.size(), data_pointers, nullptr);


	auto blob_data = blob_chunk.ToUnifiedFormat();
	RowOperations::Scatter(blob_chunk, blob_data.get(), sort_layout.blob_layout, addresses, *blob_sorting_heap,
						   sel_ptr, blob_chunk.size());
#endif
}

static unique_ptr<FunctionData> SortKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (bound_function.return_type.id() != LogicalTypeId::SORT_KEY) {
		throw BinderException("SortKey requires a SORT_KEY type");
	}

	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction SortKeyFun::GetFunction() {
	ScalarFunction fun("sort_key", {}, LogicalTypeId::SORT_KEY, SortKeyExecute, SortKeyBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

} // namespace duckdb
