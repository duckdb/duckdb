#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

static void ListIntersectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto row_count = args.size();

	// Handle NULL return type case
	if (result.GetType() == LogicalType::SQLNULL) {
		ConstantVector::SetNull(result, count_t(row_count));
		return;
	}

	auto &l_vec = args.data[0];
	auto &r_vec = args.data[1];

	const auto l_size = ListVector::GetListSize(l_vec);
	const auto r_size = ListVector::GetListSize(r_vec);

	auto &l_child = ListVector::GetChildMutable(l_vec);
	auto &r_child = ListVector::GetChildMutable(r_vec);

	auto l_entries = l_vec.Values<list_entry_t>(row_count);
	auto r_entries = r_vec.Values<list_entry_t>(row_count);

	// child element type is generic T, so we cannot use Values<>; UnifiedVectorFormat
	// gives us per-row validity + selection regardless of the child vector kind.
	UnifiedVectorFormat l_child_format;
	UnifiedVectorFormat r_child_format;
	l_child.ToUnifiedFormat(l_size, l_child_format);
	r_child.ToUnifiedFormat(r_size, r_child_format);

	Vector l_sortkey_vec(LogicalType::BLOB, l_size);
	Vector r_sortkey_vec(LogicalType::BLOB, r_size);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	CreateSortKeyHelpers::CreateSortKey(l_child, l_size, order_modifiers, l_sortkey_vec);
	CreateSortKeyHelpers::CreateSortKey(r_child, r_size, order_modifiers, r_sortkey_vec);

	const auto l_sortkey_ptr = FlatVector::GetData<string_t>(l_sortkey_vec);
	const auto r_sortkey_ptr = FlatVector::GetData<string_t>(r_sortkey_vec);

	string_set_t set;
	string_set_t result_set;
	string_map_t<idx_t> key_to_index_map;

	auto result_data = FlatVector::Writer<list_entry_t>(result, row_count);
	for (idx_t i = 0; i < row_count; i++) {
		auto l_entry = l_entries[i];
		auto r_entry = r_entries[i];

		if (!l_entry.IsValid()) {
			result_data.WriteNull();
			continue;
		}

		auto list = result_data.WriteDynamicList();
		if (!r_entry.IsValid()) {
			continue;
		}

		const auto &l_list = l_entry.GetValue();
		const auto &r_list = r_entry.GetValue();

		if (l_list.length == 0 || r_list.length == 0) {
			continue;
		}

		set.clear();
		result_set.clear();
		key_to_index_map.clear();

		// Choose which side to hash and which to iterate
		const bool use_l_for_hash = l_list.length <= r_list.length;
		const auto &hash_list = use_l_for_hash ? l_list : r_list;
		const auto &iter_list = use_l_for_hash ? r_list : l_list;
		const auto &hash_fmt = use_l_for_hash ? l_child_format : r_child_format;
		const auto &iter_fmt = use_l_for_hash ? r_child_format : l_child_format;
		const auto *hash_keys = use_l_for_hash ? l_sortkey_ptr : r_sortkey_ptr;
		const auto *iter_keys = use_l_for_hash ? r_sortkey_ptr : l_sortkey_ptr;

		for (idx_t j = 0; j < hash_list.length; j++) {
			const idx_t h_idx = hash_list.offset + j;
			const idx_t h_entry = hash_fmt.sel->get_index(h_idx);
			if (!hash_fmt.validity.RowIsValid(h_entry)) {
				continue;
			}
			const auto &key = hash_keys[h_entry];
			set.insert(key);
			if (use_l_for_hash) {
				key_to_index_map[key] = h_idx;
			}
		}

		// Iterate the chosen side, but ALWAYS emit a LEFT index
		const idx_t row_max_length = MinValue(l_list.length, r_list.length);
		SelectionVector row_sel(row_max_length);
		idx_t row_result_length = 0;
		for (idx_t j = 0; j < iter_list.length; j++) {
			const idx_t it_idx = iter_list.offset + j;
			const idx_t it_entry = iter_fmt.sel->get_index(it_idx);
			if (!iter_fmt.validity.RowIsValid(it_entry)) {
				continue;
			}

			const auto &key = iter_keys[it_entry];
			if (set.find(key) == set.end() || result_set.find(key) != result_set.end()) {
				continue;
			}
			result_set.insert(key);

			const idx_t emit_left_idx = use_l_for_hash ? key_to_index_map[key] : it_idx;

			row_sel.set_index(row_result_length, emit_left_idx);
			row_result_length++;
		}

		list.Append(l_child, row_sel, l_size, 0, row_result_length);
	}
}
static unique_ptr<FunctionData> ListIntersectBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	D_ASSERT(bound_function.GetArguments().size() == 2);
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	arguments[1] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[1]));
	return nullptr;
}

ScalarFunction ListIntersectFun::GetFunction() {
	auto fun =
	    ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::LIST(LogicalType::TEMPLATE("T"))},
	                   LogicalType::LIST(LogicalType::TEMPLATE("T")), ListIntersectFunction, ListIntersectBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
