#include "duckdb/common/types/row/tuple_data_layout.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/sorting/sort_key.hpp"

namespace duckdb {

TupleDataLayout::TupleDataLayout()
    : sort_key_type(SortKeyType::INVALID), flag_width(0), data_width(0), aggr_width(0), row_width(0),
      all_constant(true), heap_size_offset(0) {
}

TupleDataLayout TupleDataLayout::Copy() const {
	TupleDataLayout result;
	result.types = this->types;
	result.aggregates = this->aggregates;
	result.sort_key_type = this->sort_key_type;
	if (this->struct_layouts) {
		result.struct_layouts = make_uniq<unordered_map<idx_t, TupleDataLayout>>();
		for (const auto &entry : *this->struct_layouts) {
			result.struct_layouts->emplace(entry.first, entry.second.Copy());
		}
	}
	result.flag_width = this->flag_width;
	result.data_width = this->data_width;
	result.aggr_width = this->aggr_width;
	result.sort_width = this->sort_width;
	result.sort_skippable_bytes = this->sort_skippable_bytes;
	result.row_width = this->row_width;
	result.offsets = this->offsets;
	result.all_constant = this->all_constant;
	result.heap_size_offset = this->heap_size_offset;
	result.aggr_destructor_idxs = this->aggr_destructor_idxs;
	return result;
}

void TupleDataLayout::Initialize(vector<LogicalType> types_p, Aggregates aggregates_p, bool align, bool heap_offset_p) {
	sort_key_type = SortKeyType::INVALID;
	offsets.clear();
	aggr_destructor_idxs.clear();
	types = std::move(types_p);
	all_constant = true;
	sort_skippable_bytes.clear();
	variable_columns.clear();

	// Null mask at the front - 1 bit per value.
	flag_width = ValidityBytes::ValidityMaskSize(types.size());
	row_width = flag_width;

	// Whether all columns are constant size.
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		const auto &type = types[col_idx];
		if (type.InternalType() == PhysicalType::STRUCT) {
			// structs are recursively stored as a TupleDataLayout again
			const auto &child_types = StructType::GetChildTypes(type);
			vector<LogicalType> child_type_vector;
			child_type_vector.reserve(child_types.size());
			for (auto &ct : child_types) {
				child_type_vector.emplace_back(ct.second);
			}
			if (!struct_layouts) {
				struct_layouts = make_uniq<unordered_map<idx_t, TupleDataLayout>>();
			}
			auto struct_entry = struct_layouts->emplace(col_idx, TupleDataLayout());
			struct_entry.first->second.Initialize(std::move(child_type_vector), false, false);

			if (!struct_entry.first->second.AllConstant()) {
				all_constant = false;
				variable_columns.push_back(col_idx);
			}
		} else if (!TypeIsConstantSize(type.InternalType())) {
			all_constant = false;
			variable_columns.push_back(col_idx);
		}
	}

	// This enables pointer recomputation for out-of-core.
	if (heap_offset_p && !all_constant) {
		heap_size_offset = row_width;
		row_width += sizeof(idx_t);
	}

	// Data columns. No alignment required.
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		const auto &type = types[col_idx];
		offsets.push_back(row_width);
		const auto physical_type = type.InternalType();
		if (TypeIsConstantSize(physical_type) || physical_type == PhysicalType::VARCHAR) {
			row_width += GetTypeIdSize(physical_type);
		} else if (physical_type == PhysicalType::STRUCT) {
			// Just get the size of the TupleDataLayout of the struct
			row_width += GetStructLayout(col_idx).GetRowWidth();
		} else {
			// Variable size types use pointers to the actual data (can be swizzled).
			// Again, we would use sizeof(data_ptr_t), but this is not guaranteed to be equal to sizeof(idx_t).
			row_width += sizeof(idx_t);
		}
	}

	// Alignment padding for aggregates
#ifndef DUCKDB_ALLOW_UNDEFINED
	if (align) {
		row_width = AlignValue(row_width);
	}
#endif
	data_width = row_width - flag_width;

	// Aggregate fields.
	aggregates = std::move(aggregates_p);
	for (auto &aggregate : aggregates) {
		offsets.push_back(row_width);
		row_width += aggregate.payload_size;
#ifndef DUCKDB_ALLOW_UNDEFINED
		D_ASSERT(aggregate.payload_size == AlignValue(aggregate.payload_size));
#endif
	}
	aggr_width = row_width - data_width - flag_width;

	// Alignment padding for the next row
#ifndef DUCKDB_ALLOW_UNDEFINED
	if (align) {
		row_width = AlignValue(row_width);
	}
#endif

	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		const auto &aggr = aggregates[aggr_idx];
		if (aggr.function.destructor) {
			aggr_destructor_idxs.push_back(aggr_idx);
		}
	}
}

void TupleDataLayout::Initialize(vector<LogicalType> types_p, bool align, bool heap_offset_p) {
	Initialize(std::move(types_p), Aggregates(), align, heap_offset_p);
}

void TupleDataLayout::Initialize(Aggregates aggregates_p, bool align, bool heap_offset_p) {
	Initialize(vector<LogicalType>(), std::move(aggregates_p), align, heap_offset_p);
}

void TupleDataLayout::Initialize(const vector<BoundOrderByNode> &orders, const LogicalType &type, bool has_payload) {
	// Reset state
	types.clear();
	aggregates.clear();
	struct_layouts.reset();
	flag_width = DConstants::INVALID_INDEX;
	data_width = DConstants::INVALID_INDEX;
	aggr_width = DConstants::INVALID_INDEX;
	offsets.clear();
	sort_skippable_bytes.clear();
	all_constant = true;
	heap_size_offset = DConstants::INVALID_INDEX;
	aggr_destructor_idxs.clear();

	// Type is determined by "create_sort_key", if it is <= 8 we get a bigint
	types.push_back(type);

	// Compute row width
	sort_width = 0;
	bool all_valid_and_truely_constant = true;
	for (idx_t order_idx = 0; order_idx < orders.size(); order_idx++) {
		const auto &order = orders[order_idx];
		const auto &logical_type = order.expression->return_type;
		const auto physical_type = logical_type.InternalType();

		if (all_valid_and_truely_constant && order.stats && !order.stats->CanHaveNull() &&
		    TypeIsConstantSize(physical_type) && sort_width < 7) {
			// We don't have to sort by this byte, all values are valid
			sort_skippable_bytes.emplace_back(sort_width);
		} else {
			all_valid_and_truely_constant = false;
		}

		if (TypeIsConstantSize(physical_type)) {
			// NULL byte + fixed-width type
			sort_width += 1 + GetTypeIdSize(physical_type);
		} else if (logical_type == LogicalType::VARCHAR && order.stats &&
		           StringStats::HasMaxStringLength(*order.stats)) {
			// NULL byte + maximum string length + string delimiter
			sort_width += 1 + StringStats::MaxStringLength(*order.stats) + 1;
		} else {
			// We don't know how long the key will be
			sort_width = DConstants::INVALID_INDEX;
			break;
		}
	}

	// Set row width and sort key type accordingly
	idx_t temp_row_width = type.id() == LogicalTypeId::BIGINT ? 8 : sort_width;
	if (sort_width != DConstants::INVALID_INDEX && has_payload) {
		temp_row_width += 8;
	}
	if (temp_row_width <= 8) {
		D_ASSERT(!has_payload);
		row_width = 8;
		sort_key_type = SortKeyType::NO_PAYLOAD_FIXED_8;
	} else if (temp_row_width <= 16) {
		row_width = 16;
		sort_key_type = has_payload ? SortKeyType::PAYLOAD_FIXED_16 : SortKeyType::NO_PAYLOAD_FIXED_16;
	} else if (temp_row_width <= 24) {
		row_width = 24;
		sort_key_type = has_payload ? SortKeyType::PAYLOAD_FIXED_24 : SortKeyType::NO_PAYLOAD_FIXED_24;
	} else if (temp_row_width <= 32) {
		row_width = 32;
		sort_key_type = has_payload ? SortKeyType::PAYLOAD_FIXED_32 : SortKeyType::NO_PAYLOAD_FIXED_32;
	} else {
		row_width = 32;
		sort_key_type = has_payload ? SortKeyType::PAYLOAD_VARIABLE_32 : SortKeyType::NO_PAYLOAD_VARIABLE_32;

		// Variable-size sort key, also set these properties
		all_constant = false;
		heap_size_offset = has_payload ? SortKey<SortKeyType::PAYLOAD_VARIABLE_32>::HEAP_SIZE_OFFSET
		                               : SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>::HEAP_SIZE_OFFSET;
	}
}

bool TupleDataLayout::IsSortKeyLayout() const {
	return sort_key_type != SortKeyType::INVALID;
}

} // namespace duckdb
