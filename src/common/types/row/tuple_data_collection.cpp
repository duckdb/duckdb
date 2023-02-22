#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_allocator.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

typedef void (*tuple_data_scatter_function_t)(const Vector &source, const UnifiedVectorFormat &source_data,
                                              const idx_t source_offset, const idx_t count,
                                              const TupleDataLayout &layout, Vector &row_locations,
                                              Vector &heap_row_locations, const idx_t col_idx,
                                              const vector<TupleDataScatterFunction> &child_functions);

struct TupleDataScatterFunction {
	tuple_data_scatter_function_t function;
	vector<TupleDataScatterFunction> child_functions;
};

typedef void (*tuple_data_gather_function_t)(Vector &row_locations, Vector &heap_row_locations,
                                             const TupleDataLayout &layout, const idx_t col_idx, const idx_t count,
                                             Vector &target, const vector<TupleDataGatherFunction> &child_functions);

struct TupleDataGatherFunction {
	tuple_data_gather_function_t function;
	vector<TupleDataGatherFunction> child_functions;
};

TupleDataCollection::TupleDataCollection(ClientContext &context, vector<LogicalType> types,
                                         vector<AggregateObject> aggregates, bool align) {
	Initialize(context, std::move(types), std::move(aggregates), align);
}

TupleDataCollection::TupleDataCollection(ClientContext &context, vector<LogicalType> types, bool align)
    : TupleDataCollection(context, std::move(types), {}, align) {
}

TupleDataCollection::TupleDataCollection(ClientContext &context, vector<AggregateObject> aggregates, bool align)
    : TupleDataCollection(context, {}, std::move(aggregates), align) {
}

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state) {
	append_state.vector_data.resize(layout.ColumnCount());
}

void TupleDataCollection::Append(TupleDataAppendState &append_state, DataChunk &chunk) {
	if (segments.empty()) {
		segments.emplace_back(allocator);
	}
	D_ASSERT(segments.size() == 1);                                    // Cannot append after Combine
	D_ASSERT(append_state.vector_data.size() == layout.ColumnCount()); // Needs InitializeAppend
	D_ASSERT(chunk.ColumnCount() == layout.ColumnCount());             // Chunk needs to adhere to schema
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		chunk.data[i].ToUnifiedFormat(chunk.size(), append_state.vector_data[i]);
	}
	if (!layout.AllConstant()) {
		ComputeEntrySizes(append_state, chunk);
	}
	allocator->Build(append_state, count, segments.back());

	// Set the validity mask for each row before inserting data
	auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.row_locations);
	for (idx_t i = 0; i < count; ++i) {
		ValidityBytes(row_locations[i]).SetAllValid(layout.ColumnCount());
	}
}

void TupleDataCollection::ComputeEntrySizes(TupleDataAppendState &append_state, DataChunk &chunk) {
	auto entry_sizes = FlatVector::GetData<idx_t>(append_state.heap_row_sizes);
	memset(entry_sizes, 0, chunk.size() * sizeof(idx_t));

	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		auto type = layout.GetTypes()[i].InternalType();
		if (TypeIsConstantSize(type)) {
			continue;
		}

		switch (type) {
		case PhysicalType::VARCHAR:
		case PhysicalType::LIST:
		case PhysicalType::STRUCT:
			RowOperations::ComputeEntrySizes(chunk.data[i], append_state.vector_data[i], entry_sizes, chunk.size(),
			                                 chunk.size(), *FlatVector::IncrementalSelectionVector());
			break;
		default:
			throw InternalException("Unsupported type for RowOperations::ComputeEntrySizes");
		}
	}
}

void TupleDataCollection::Initialize(ClientContext &context, vector<LogicalType> types,
                                     vector<AggregateObject> aggregates, bool align) {
	D_ASSERT(!types.empty());
	layout.Initialize(std::move(types), std::move(aggregates), align);
	allocator = make_shared<TupleDataAllocator>(context, layout);
	this->count = 0;

	scatter_functions.reserve(layout.GetTypes().size());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		scatter_functions.emplace_back(GetScatterFunction(layout, col_idx));
	}

	gather_functions.reserve(layout.GetTypes().size());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		gather_functions.emplace_back(GetGatherFunction(layout, col_idx));
	}
}

template <class T>
static inline void TupleDataValueScatter(const T &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                         data_ptr_t &heap_location) {
	Store<T>(source, row_location + offset_in_row);
}

template <>
inline void TupleDataValueScatter(const string_t &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                  data_ptr_t &heap_location) {
	memcpy(heap_location, source.GetDataUnsafe(), source.GetSize());
	Store<string_t>(string_t((const char *)heap_location, source.GetSize()), row_location + offset_in_row);
	heap_location += source.GetSize();
}

// TODO: nested types value scatter

template <class T>
static void TemplatedTupleDataScatter(const Vector &source, const UnifiedVectorFormat &source_data,
                                      const idx_t source_offset, const idx_t count, const TupleDataLayout &layout,
                                      Vector &row_locations, Vector &heap_row_locations, const idx_t col_idx,
                                      const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto sel = *source_data.sel;
	const auto data = (T *)source_data.data;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_row_locations);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	if (validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i + source_offset);
			TupleDataValueScatter<T>(data[idx], target_locations[i], offset_in_row, target_heap_locations[i]);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i + source_offset);
			if (validity.RowIsValid(idx)) {
				TupleDataValueScatter<T>(data[idx], target_locations[i], offset_in_row, target_heap_locations[i]);
			} else {
				TupleDataValueScatter<T>(NullValue<T>(), target_locations[i], offset_in_row, target_heap_locations[i]);
				ValidityBytes(target_locations[i]).SetValidUnsafe(col_idx);
			}
		}
	}
}

static void StructTupleDataScatter(const Vector &source, const UnifiedVectorFormat &source_data,
                                   const idx_t source_offset, const idx_t count, const TupleDataLayout &layout,
                                   Vector &row_locations, Vector &heap_row_locations, const idx_t col_idx,
                                   const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto sel = *source_data.sel;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Set validity of the struct
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i + source_offset);
		if (!validity.RowIsValid(idx)) {
			ValidityBytes(target_locations[i]).SetValidUnsafe(col_idx);
		}
	}

	// Create a Vector of pointers pointing to the start of the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER, count);
	auto struct_target_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < count; i++) {
		struct_target_locations[i] = target_locations[i] + offset_in_row;
	}

	D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
	const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
	auto &struct_sources = StructVector::GetEntries(source);
	D_ASSERT(struct_layout.ColumnCount() == struct_sources.size());

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		const auto &struct_source = struct_sources[struct_col_idx];
		UnifiedVectorFormat struct_source_data;
		struct_source->ToUnifiedFormat(count, struct_source_data);
		const auto &struct_scatter_function = child_functions[col_idx];
		struct_scatter_function.function(*struct_sources[struct_col_idx], struct_source_data, source_offset, count,
		                                 struct_layout, struct_row_locations, heap_row_locations, struct_col_idx,
		                                 struct_scatter_function.child_functions);
	}
}

TupleDataScatterFunction TupleDataCollection::GetScatterFunction(const TupleDataLayout &layout, idx_t col_idx) {
	const auto &type = layout.GetTypes()[col_idx];

	TupleDataScatterFunction result;
	tuple_data_scatter_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = TemplatedTupleDataScatter<bool>;
		break;
	case PhysicalType::INT8:
		function = TemplatedTupleDataScatter<int8_t>;
		break;
	case PhysicalType::INT16:
		function = TemplatedTupleDataScatter<int16_t>;
		break;
	case PhysicalType::INT32:
		function = TemplatedTupleDataScatter<int32_t>;
		break;
	case PhysicalType::INT64:
		function = TemplatedTupleDataScatter<int64_t>;
		break;
	case PhysicalType::INT128:
		function = TemplatedTupleDataScatter<hugeint_t>;
		break;
	case PhysicalType::UINT8:
		function = TemplatedTupleDataScatter<uint8_t>;
		break;
	case PhysicalType::UINT16:
		function = TemplatedTupleDataScatter<uint16_t>;
		break;
	case PhysicalType::UINT32:
		function = TemplatedTupleDataScatter<uint32_t>;
		break;
	case PhysicalType::UINT64:
		function = TemplatedTupleDataScatter<uint64_t>;
		break;
	case PhysicalType::FLOAT:
		function = TemplatedTupleDataScatter<float>;
		break;
	case PhysicalType::DOUBLE:
		function = TemplatedTupleDataScatter<double>;
		break;
	case PhysicalType::INTERVAL:
		function = TemplatedTupleDataScatter<interval_t>;
		break;
	case PhysicalType::VARCHAR:
		function = TemplatedTupleDataScatter<string_t>;
		break;
	case PhysicalType::STRUCT: {
		function = StructTupleDataScatter;
		D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
		const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
			result.child_functions.push_back(GetScatterFunction(struct_layout, struct_col_idx));
		}
		break;
	}
		//	case PhysicalType::LIST: {
		//		function = TemplatedTupleDataCopy<list_entry_t>;
		//		auto child_function = GetCopyFunction(ListType::GetChildType(type));
		//		result.child_functions.push_back(child_function);
		//		break;
		//	}
	default:
		throw InternalException("Unsupported type for ColumnDataCollection::GetCopyFunction");
	}
	result.function = function;
	return result;
}

// TODO: nested types value gather

template <class T>
static void TemplatedTupleDataGather(Vector &row_locations, Vector &heap_row_locations, const TupleDataLayout &layout,
                                     const idx_t col_idx, const idx_t count, Vector &target,
                                     const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto target_data = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	const auto offset_in_row = layout.GetOffsets()[col_idx];

	for (idx_t i = 0; i < count; i++) {
		ValidityBytes row_mask(source_locations[i]);
		if (row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			target_data[i] = Load<T>(source_locations[i] + offset_in_row);
		} else {
			target_validity.SetInvalid(i);
		}
	}
}

TupleDataGatherFunction TupleDataCollection::GetGatherFunction(const TupleDataLayout &layout, idx_t col_idx) {
	const auto &type = layout.GetTypes()[col_idx];

	TupleDataGatherFunction result;
	tuple_data_gather_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = TemplatedTupleDataGather<bool>;
		break;
	case PhysicalType::INT8:
		function = TemplatedTupleDataGather<int8_t>;
		break;
	case PhysicalType::INT16:
		function = TemplatedTupleDataGather<int16_t>;
		break;
	case PhysicalType::INT32:
		function = TemplatedTupleDataGather<int32_t>;
		break;
	case PhysicalType::INT64:
		function = TemplatedTupleDataGather<int64_t>;
		break;
	case PhysicalType::INT128:
		function = TemplatedTupleDataGather<hugeint_t>;
		break;
	case PhysicalType::UINT8:
		function = TemplatedTupleDataGather<uint8_t>;
		break;
	case PhysicalType::UINT16:
		function = TemplatedTupleDataGather<uint16_t>;
		break;
	case PhysicalType::UINT32:
		function = TemplatedTupleDataGather<uint32_t>;
		break;
	case PhysicalType::UINT64:
		function = TemplatedTupleDataGather<uint64_t>;
		break;
	case PhysicalType::FLOAT:
		function = TemplatedTupleDataGather<float>;
		break;
	case PhysicalType::DOUBLE:
		function = TemplatedTupleDataGather<double>;
		break;
	case PhysicalType::INTERVAL:
		function = TemplatedTupleDataGather<interval_t>;
		break;
	case PhysicalType::VARCHAR:
		function = TemplatedTupleDataGather<string_t>;
		break;
		//	case PhysicalType::STRUCT: {
		//		function = TemplatedTupleDataCopyStruct;
		//		auto &child_types = StructType::GetChildTypes(type);
		//		for (auto &kv : child_types) {
		//			result.child_functions.push_back(GetCopyFunction(kv.second));
		//		}
		//		break;
		//	}
		//	case PhysicalType::LIST: {
		//		function = TemplatedTupleDataCopy<list_entry_t>;
		//		auto child_function = GetCopyFunction(ListType::GetChildType(type));
		//		result.child_functions.push_back(child_function);
		//		break;
		//	}
	default:
		throw InternalException("Unsupported type for ColumnDataCollection::GetCopyFunction");
	}
	result.function = function;
	return result;
}

} // namespace duckdb
