#include "duckdb/common/types/row/tuple_data_layout.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

TupleDataLayout::TupleDataLayout()
    : flag_width(0), data_width(0), aggr_width(0), row_width(0), all_constant(true), heap_size_offset(0),
      has_destructor(false) {
}

TupleDataLayout TupleDataLayout::Copy() const {
	TupleDataLayout result;
	result.types = this->types;
	result.aggregates = this->aggregates;
	if (this->struct_layouts) {
		result.struct_layouts = make_uniq<unordered_map<idx_t, TupleDataLayout>>();
		for (const auto &entry : *this->struct_layouts) {
			result.struct_layouts->emplace(entry.first, entry.second.Copy());
		}
	}
	result.flag_width = this->flag_width;
	result.data_width = this->data_width;
	result.aggr_width = this->aggr_width;
	result.row_width = this->row_width;
	result.offsets = this->offsets;
	result.all_constant = this->all_constant;
	result.heap_size_offset = this->heap_size_offset;
	result.has_destructor = this->has_destructor;
	return result;
}

void TupleDataLayout::Initialize(vector<LogicalType> types_p, Aggregates aggregates_p, bool align, bool heap_offset_p) {
	offsets.clear();
	types = std::move(types_p);

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
			all_constant = all_constant && struct_entry.first->second.AllConstant();
		} else {
			all_constant = all_constant && TypeIsConstantSize(type.InternalType());
		}
	}

	// This enables pointer swizzling for out-of-core computation.
	if (heap_offset_p && !all_constant) {
		heap_size_offset = row_width;
		row_width += sizeof(uint32_t);
	}

	// Data columns. No alignment required.
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		const auto &type = types[col_idx];
		offsets.push_back(row_width);
		const auto internal_type = type.InternalType();
		if (TypeIsConstantSize(internal_type) || internal_type == PhysicalType::VARCHAR) {
			row_width += GetTypeIdSize(type.InternalType());
		} else if (internal_type == PhysicalType::STRUCT) {
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

	has_destructor = false;
	for (auto &aggr : GetAggregates()) {
		if (aggr.function.destructor) {
			has_destructor = true;
			break;
		}
	}
}

void TupleDataLayout::Initialize(vector<LogicalType> types_p, bool align, bool heap_offset_p) {
	Initialize(std::move(types_p), Aggregates(), align, heap_offset_p);
}

void TupleDataLayout::Initialize(Aggregates aggregates_p, bool align, bool heap_offset_p) {
	Initialize(vector<LogicalType>(), std::move(aggregates_p), align, heap_offset_p);
}

} // namespace duckdb
