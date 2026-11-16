#include "duckdb/execution/operator/order/top_n_boundary.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Shared boundary
//===--------------------------------------------------------------------===//
TopNBoundaryValue::TopNBoundaryValue(const vector<BoundOrderByNode> &orders,
                                     optional_ptr<DynamicFilterData> dynamic_filter)
    : dynamic_filter(dynamic_filter), boundary_vector(orders[0].expression->GetReturnType()),
      boundary_modifiers(orders[0].type, orders[0].null_order) {
}

bool TopNBoundaryValue::TryGetBoundaryValue(string &current_boundary) {
	unique_lock<mutex> l(lock, std::try_to_lock);
	if (!l.owns_lock() || boundary_value == current_boundary) {
		return false;
	}
	current_boundary = boundary_value;
	return true;
}

void TopNBoundaryValue::UpdateValue(string_t boundary_val) {
	unique_lock<mutex> l(lock);
	if (!is_set || boundary_val < string_t(boundary_value)) {
		boundary_value = boundary_val.GetString();
		is_set = true;
		if (dynamic_filter) {
			// the first key of the boundary is the bound of the dynamic filter (a NULL does not change it)
			CreateSortKeyHelpers::DecodeSortKey(boundary_val, boundary_vector, 0, boundary_modifiers);
			auto new_dynamic_value = boundary_vector.GetValue(0);
			l.unlock();
			dynamic_filter->SetValue(std::move(new_dynamic_value));
		}
	}
}

//===--------------------------------------------------------------------===//
// Thread-local selection
//===--------------------------------------------------------------------===//
TopNBoundarySelector::TopNBoundarySelector(Allocator &allocator, const vector<BoundOrderByNode> &orders_p)
    : orders(orders_p), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE),
      new_remaining_sel(STANDARD_VECTOR_SIZE) {
	vector<LogicalType> key_types;
	for (auto &order : orders) {
		key_types.push_back(order.expression->GetReturnType());
		modifiers.emplace_back(order.type, order.null_order);
	}
	compare_chunk.Initialize(allocator, key_types);
	boundary_values.Initialize(allocator, key_types);
}

void TopNBoundarySelector::Decode(const string &sort_key, DataChunk &target) const {
	target.Reset();
	CreateSortKeyHelpers::DecodeSortKey(string_t(sort_key), target, 0, modifiers);
	for (auto &col : target.data) {
		col.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

bool TopNBoundarySelector::Refresh(TopNBoundaryValue &global_boundary) {
	// An older local boundary remains valid if refreshing it would block.
	const auto boundary_updated = global_boundary.TryGetBoundaryValue(boundary_val);
	if (boundary_val.empty()) {
		// no boundary value (yet)
		return false;
	}
	if (boundary_updated) {
		// new boundary value - decode
		Decode(boundary_val, boundary_values);
	}
	return true;
}

//! Selects the rows whose left value sorts strictly before the right value in the given order
static idx_t SelectBefore(const BoundOrderByNode &order, Vector &left, Vector &right, SelectionVector &sel, idx_t count,
                          SelectionVector &true_sel, SelectionVector &false_sel) {
	if (order.null_order == OrderByNullType::NULLS_LAST) {
		if (order.type == OrderType::ASCENDING) {
			return VectorOperations::DistinctLessThan(left, right, &sel, count, &true_sel, &false_sel);
		}
		return VectorOperations::DistinctGreaterThanNullsFirst(left, right, &sel, count, &true_sel, &false_sel);
	}
	D_ASSERT(order.null_order == OrderByNullType::NULLS_FIRST);
	if (order.type == OrderType::ASCENDING) {
		return VectorOperations::DistinctLessThanNullsFirst(left, right, &sel, count, &true_sel, &false_sel);
	}
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, &true_sel, &false_sel);
}

idx_t TopNBoundarySelector::Select(DataChunk &keys, DataChunk &bound, optional_ptr<const SelectionVector> sel,
                                   idx_t count, TopNBoundaryTies ties, TopNBoundaryRowOrder row_order,
                                   SelectionVector &result) {
	bound.SetChildCardinality(keys.size());
	idx_t final_count = 0;
	// the number of runs of rows that were appended to the result: each run is in the order of the input
	idx_t appended_runs = 0;
	SelectionVector remaining_sel(nullptr);
	if (sel) {
		remaining_sel.Initialize(*sel);
	}
	idx_t remaining_count = count;
	const auto key_count = orders.size();
	for (idx_t i = 0; i < key_count && remaining_count > 0; i++) {
		auto &order = orders[i];
		if (remaining_sel.data()) {
			compare_chunk.data[i].Slice(keys.data[i], remaining_sel, remaining_count);
		} else {
			compare_chunk.data[i].Reference(keys.data[i]);
		}
		const auto is_last = i + 1 == key_count;
		if (is_last && ties == TopNBoundaryTies::INCLUDE) {
			// before or equal to the boundary: the rows that the boundary does not sort before
			const auto after_count = SelectBefore(order, bound.data[i], compare_chunk.data[i], remaining_sel,
			                                      remaining_count, false_sel, true_sel);
			const auto pass_count = remaining_count - after_count;
			if (pass_count > 0) {
				memcpy(result.data() + final_count, true_sel.data(), pass_count * sizeof(sel_t));
				final_count += pass_count;
				appended_runs++;
			}
			break;
		}
		const auto true_count = SelectBefore(order, compare_chunk.data[i], bound.data[i], remaining_sel,
		                                     remaining_count, true_sel, false_sel);
		if (true_count > 0) {
			memcpy(result.data() + final_count, true_sel.data(), true_count * sizeof(sel_t));
			final_count += true_count;
			appended_runs++;
		}
		const auto false_count = remaining_count - true_count;
		if (is_last || false_count == 0) {
			break;
		}
		// the rows that are equal to the boundary on this key continue with the next key
		compare_chunk.data[i].Slice(keys.data[i], false_sel, false_count);
		remaining_count = VectorOperations::NotDistinctFrom(compare_chunk.data[i], bound.data[i], &false_sel,
		                                                    false_count, &new_remaining_sel, nullptr);
		remaining_sel.Initialize(new_remaining_sel);
	}
	if (row_order == TopNBoundaryRowOrder::INPUT_ORDER && appended_runs > 1) {
		// the rows were selected key by key: restore the order of the input
		if (!selected_rows) {
			selected_rows = make_unsafe_uniq_array<bool>(STANDARD_VECTOR_SIZE);
		}
		const auto row_count = keys.size();
		memset(selected_rows.get(), 0, row_count * sizeof(bool));
		for (idx_t i = 0; i < final_count; i++) {
			selected_rows[result.get_index(i)] = true;
		}
		idx_t result_idx = 0;
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			result.set_index(result_idx, row_idx);
			result_idx += selected_rows[row_idx];
		}
		D_ASSERT(result_idx == final_count);
	}
	return final_count;
}

} // namespace duckdb
