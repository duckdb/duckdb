#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

static idx_t CalculateMaxResultLength(idx_t row_count, const UnifiedVectorFormat &l_format,
                                      const UnifiedVectorFormat &r_format, const list_entry_t *l_entries,
                                      const list_entry_t *r_entries) {
	idx_t max_result_length = 0;
	for (idx_t i = 0; i < row_count; i++) {
		const auto l_idx = l_format.sel->get_index(i);
		const auto r_idx = r_format.sel->get_index(i);

		if (l_format.validity.RowIsValid(l_idx) && r_format.validity.RowIsValid(r_idx)) {
			const auto &l_list = l_entries[l_idx];
			const auto &r_list = r_entries[r_idx];
			max_result_length += MinValue(l_list.length, r_list.length);
		}
	}
	return max_result_length;
}

static void ListIntersectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto row_count = args.size();

	// Handle NULL return type case
	if (result.GetType() == LogicalType::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	auto &l_vec = args.data[0];
	auto &r_vec = args.data[1];

	const auto l_size = ListVector::GetListSize(l_vec);
	const auto r_size = ListVector::GetListSize(r_vec);

	auto &l_child = ListVector::GetEntry(l_vec);
	auto &r_child = ListVector::GetEntry(r_vec);

	const auto current_left_child_type = l_child.GetType();

	UnifiedVectorFormat l_format;
	UnifiedVectorFormat r_format;

	l_vec.ToUnifiedFormat(row_count, l_format);
	r_vec.ToUnifiedFormat(row_count, r_format);

	const auto l_entries = UnifiedVectorFormat::GetData<list_entry_t>(l_format);
	const auto r_entries = UnifiedVectorFormat::GetData<list_entry_t>(r_format);

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

	auto *result_data = FlatVector::GetData<list_entry_t>(result);
	auto &result_entry = ListVector::GetEntry(result);

	string_set_t set;
	string_set_t result_set;
	string_map_t<idx_t> key_to_index_map;

	const idx_t max_result_length = CalculateMaxResultLength(row_count, l_format, r_format, l_entries, r_entries);

	ListVector::Reserve(result, max_result_length);
	ListVector::SetListSize(result, max_result_length);

	SelectionVector result_sel(max_result_length);
	ValidityMask result_entry_validity_mask(max_result_length);
	idx_t offset = 0;

	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < row_count; i++) {
		const auto l_idx = l_format.sel->get_index(i);
		const auto r_idx = r_format.sel->get_index(i);

		const bool l_valid = l_format.validity.RowIsValid(l_idx);
		const bool r_valid = r_format.validity.RowIsValid(r_idx);

		result_data[i].offset = offset;

		if (!l_valid) {
			result_validity.SetInvalid(i);
			result_data[i].length = 0;
			continue;
		}
		if (!r_valid) {
			result_data[i].length = 0;
			continue;
		}

		const auto &l_list = l_entries[l_idx];
		const auto &r_list = r_entries[r_idx];

		if (l_list.length == 0 || r_list.length == 0) {
			result_data[i].length = 0;
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

		set.clear();
		key_to_index_map.clear();
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
		result_set.clear();
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

			result_sel.set_index(offset + row_result_length, emit_left_idx);
			row_result_length++;
		}

		result_data[i].length = row_result_length;
		offset += row_result_length;
	}

	ListVector::SetListSize(result, offset);

	result_entry.Slice(l_child, result_sel, offset);
	result_entry.Flatten(offset);
	FlatVector::SetValidity(result_entry, result_entry_validity_mask);

	result.SetVectorType(args.AllConstant() ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
}
static unique_ptr<FunctionData> ListIntersectBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	arguments[1] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[1]));
	return nullptr;
}

ScalarFunction ListIntersectFun::GetFunction() {
	auto fun =
	    ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::LIST(LogicalType::TEMPLATE("T"))},
	                   LogicalType::LIST(LogicalType::TEMPLATE("T")), ListIntersectFunction, ListIntersectBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
