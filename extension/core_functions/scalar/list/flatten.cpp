#include "core_functions/scalar/list_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

namespace {

void ListFlattenFunction(DataChunk &args, ExpressionState &, Vector &result) {
	const auto flat_list_data = FlatVector::GetData<list_entry_t>(result);
	auto &flat_list_mask = FlatVector::Validity(result);

	UnifiedVectorFormat outer_format;
	UnifiedVectorFormat inner_format;
	UnifiedVectorFormat items_format;

	// Setup outer vec;
	auto &outer_vec = args.data[0];
	const auto outer_count = args.size();
	outer_vec.ToUnifiedFormat(outer_count, outer_format);

	// Special case: outer list is all-null
	if (outer_vec.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(outer_vec);
		return;
	}

	// Setup inner vec
	auto &inner_vec = ListVector::GetEntry(outer_vec);
	const auto inner_count = ListVector::GetListSize(outer_vec);
	inner_vec.ToUnifiedFormat(inner_count, inner_format);

	// Special case: inner list is all-null
	if (inner_vec.GetType().id() == LogicalTypeId::SQLNULL) {
		for (idx_t outer_raw_idx = 0; outer_raw_idx < outer_count; outer_raw_idx++) {
			const auto outer_idx = outer_format.sel->get_index(outer_raw_idx);
			if (!outer_format.validity.RowIsValid(outer_idx)) {
				flat_list_mask.SetInvalid(outer_raw_idx);
				continue;
			}
			flat_list_data[outer_raw_idx].offset = 0;
			flat_list_data[outer_raw_idx].length = 0;
		}
		if (args.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		return;
	}

	// Setup items vec
	auto &items_vec = ListVector::GetEntry(inner_vec);
	const auto items_count = ListVector::GetListSize(inner_vec);
	items_vec.ToUnifiedFormat(items_count, items_format);

	// First pass: Figure out the total amount of items.
	// This can be more than items_count if the inner list reference the same item(s) multiple times.

	idx_t total_items = 0;

	const auto outer_data = UnifiedVectorFormat::GetData<list_entry_t>(outer_format);
	const auto inner_data = UnifiedVectorFormat::GetData<list_entry_t>(inner_format);

	for (idx_t outer_raw_idx = 0; outer_raw_idx < outer_count; outer_raw_idx++) {
		const auto outer_idx = outer_format.sel->get_index(outer_raw_idx);

		if (!outer_format.validity.RowIsValid(outer_idx)) {
			continue;
		}

		const auto &outer_entry = outer_data[outer_idx];

		for (idx_t inner_raw_idx = outer_entry.offset; inner_raw_idx < outer_entry.offset + outer_entry.length;
		     inner_raw_idx++) {
			const auto inner_idx = inner_format.sel->get_index(inner_raw_idx);

			if (!inner_format.validity.RowIsValid(inner_idx)) {
				continue;
			}

			const auto &inner_entry = inner_data[inner_idx];

			total_items += inner_entry.length;
		}
	}

	// Now we know the total amount of items, we can create our selection vector.
	SelectionVector sel(total_items);
	idx_t sel_idx = 0;

	// Second pass: Fill the selection vector (and the result list entries)

	for (idx_t outer_raw_idx = 0; outer_raw_idx < outer_count; outer_raw_idx++) {
		const auto outer_idx = outer_format.sel->get_index(outer_raw_idx);

		if (!outer_format.validity.RowIsValid(outer_idx)) {
			flat_list_mask.SetInvalid(outer_raw_idx);
			continue;
		}

		const auto &outer_entry = outer_data[outer_idx];

		list_entry_t list_entry = {sel_idx, 0};

		for (idx_t inner_raw_idx = outer_entry.offset; inner_raw_idx < outer_entry.offset + outer_entry.length;
		     inner_raw_idx++) {
			const auto inner_idx = inner_format.sel->get_index(inner_raw_idx);

			if (!inner_format.validity.RowIsValid(inner_idx)) {
				continue;
			}

			const auto &inner_entry = inner_data[inner_idx];

			list_entry.length += inner_entry.length;

			for (idx_t elem_raw_idx = inner_entry.offset; elem_raw_idx < inner_entry.offset + inner_entry.length;
			     elem_raw_idx++) {
				const auto elem_idx = items_format.sel->get_index(elem_raw_idx);

				sel.set_index(sel_idx, elem_idx);
				sel_idx++;
			}
		}

		// Assign the result list entry
		flat_list_data[outer_raw_idx] = list_entry;
	}

	// Now assing the result
	ListVector::SetListSize(result, sel_idx);

	auto &result_child_vector = ListVector::GetEntry(result);
	result_child_vector.Slice(items_vec, sel, sel_idx);
	result_child_vector.Flatten(sel_idx);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

unique_ptr<BaseStatistics> ListFlattenStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &list_child_stats = ListStats::GetChildStats(child_stats[0]);
	auto child_copy = list_child_stats.Copy();
	child_copy.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	return child_copy.ToUnique();
}

} // namespace

ScalarFunction ListFlattenFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::LIST(LogicalType::TEMPLATE("T")))},
	                      LogicalType::LIST(LogicalType::TEMPLATE("T")), ListFlattenFunction, nullptr, nullptr,
	                      ListFlattenStats);
}

} // namespace duckdb
