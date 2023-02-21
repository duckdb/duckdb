#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

typedef void (*tuple_data_scatter_function_t)(const UnifiedVectorFormat &source_data, const idx_t source_offset,
                                              const idx_t count, Vector &row_locations, Vector &heap_row_locations,
                                              const idx_t col_idx, const idx_t offset_in_row);

struct TupleDataScatterFunction {
	tuple_data_scatter_function_t function;
	vector<TupleDataScatterFunction> child_functions;
};

// TODO
typedef void (*tuple_data_gather_function_t)(const UnifiedVectorFormat &source_data, const idx_t source_offset,
                                             const idx_t count, Vector &row_locations, Vector &heap_row_locations,
                                             const idx_t col_idx, const idx_t offset_in_row);

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
	D_ASSERT(append_state.vector_data.size() == layout.ColumnCount());
	D_ASSERT(chunk.ColumnCount() == layout.ColumnCount());
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		chunk.data[i].ToUnifiedFormat(chunk.size(), append_state.vector_data[i]);
	}
	if (!layout.AllConstant()) {
		ComputeEntrySizes(append_state, chunk);
	}
	allocator->Build(append_state, count, segments);

	// Set the validity mask for each row before inserting data
	auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.row_locations);
	for (idx_t i = 0; i < count; ++i) {
		ValidityBytes(row_locations[i]).SetAllValid(layout.ColumnCount());
	}
}

void TupleDataCollection::ComputeEntrySizes(TupleDataAppendState &append_state, DataChunk &chunk) {
	auto entry_sizes = FlatVector::GetData<idx_t>(append_state.heap_row_sizes);
	std::fill_n(entry_sizes, chunk.size(), sizeof(uint32_t));
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
	for (auto &type : layout.GetTypes()) {
		scatter_functions.push_back(GetScatterFunction(type));
	}
}

template <class T>
static inline void TupleDataValueScatter(const T &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                         data_ptr_t &heap_location) {
	Store<T>(source, row_location + offset_in_row);
}

template <>
static inline void TupleDataValueScatter(const string_t &source, const data_ptr_t &row_location,
                                         const idx_t offset_in_row, data_ptr_t &heap_location) {
	memcpy(heap_location, source.GetDataUnsafe(), source.GetSize());
	Store<string_t>(string_t((const char *)heap_location, source.GetSize()), row_location + offset_in_row);
	heap_location += source.GetSize();
}

// TODO: string/nested types value copy

template <class T>
static void TemplatedTupleDataScatter(const UnifiedVectorFormat &source_data, const idx_t source_offset,
                                      const idx_t count, Vector &row_locations, Vector &heap_row_locations,
                                      const idx_t col_idx, const idx_t offset_in_row) {
	// Source
	const auto sel = *source_data.sel;
	const auto data = (T *)source_data.data;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_row_locations);

	if (!validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i + source_offset);
			if (validity.RowIsValid(idx)) {
				TupleDataValueScatter<T>(data[idx], target_locations[i], offset_in_row, target_heap_locations[i]);
			} else {
				TupleDataValueScatter<T>(NullValue<T>(), target_locations[i], offset_in_row, target_heap_locations[i]);
				ValidityBytes(target_locations[i]).SetValidUnsafe(col_idx);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i + source_offset);
			TupleDataValueScatter<T>(data[idx], target_locations[i], offset_in_row, target_heap_locations[i]);
		}
	}
}

TupleDataScatterFunction TupleDataCollection::GetScatterFunction(const LogicalType &type) {
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
		//	default:
		//		throw InternalException("Unsupported type for ColumnDataCollection::GetCopyFunction");
	}
	result.function = function;
	return result;
}

} // namespace duckdb
