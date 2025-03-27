#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_buffer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/appender/list.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ArrowAppender
//===--------------------------------------------------------------------===//

ArrowAppender::ArrowAppender(vector<LogicalType> types_p, const idx_t initial_capacity, ClientProperties options,
                             unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_type_cast)
    : types(std::move(types_p)), options(options) {
	for (idx_t i = 0; i < types.size(); i++) {
		unique_ptr<ArrowAppendData> entry;
		bool bitshift_boolean = types[i].id() == LogicalTypeId::BOOLEAN && !options.arrow_lossless_conversion;
		if (extension_type_cast.find(i) != extension_type_cast.end() && !bitshift_boolean) {
			entry = InitializeChild(types[i], initial_capacity, options, extension_type_cast[i]);
		} else {
			entry = InitializeChild(types[i], initial_capacity, options);
		}
		root_data.push_back(std::move(entry));
	}
}

ArrowAppender::~ArrowAppender() {
}

//! Append a data chunk to the underlying arrow array
void ArrowAppender::Append(DataChunk &input, const idx_t from, const idx_t to, const idx_t input_size) {
	D_ASSERT(types == input.GetTypes());
	D_ASSERT(to >= from);
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		if (root_data[i]->extension_data && root_data[i]->extension_data->duckdb_to_arrow) {
			Vector input_data(root_data[i]->extension_data->GetInternalType());
			root_data[i]->extension_data->duckdb_to_arrow(*options.client_context, input.data[i], input_data,
			                                              input_size);
			root_data[i]->append_vector(*root_data[i], input_data, from, to, input_size);
		} else {
			root_data[i]->append_vector(*root_data[i], input.data[i], from, to, input_size);
		}
	}
	row_count += to - from;
}

idx_t ArrowAppender::RowCount() const {
	return row_count;
}

void ArrowAppender::ReleaseArray(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	auto holder = static_cast<ArrowAppendData *>(array->private_data);
	for (int64_t i = 0; i < array->n_children; i++) {
		auto child = array->children[i];
		if (!child->release) {
			// Child was moved out of the array
			continue;
		}
		child->release(child);
		D_ASSERT(!child->release);
	}
	if (array->dictionary && array->dictionary->release) {
		array->dictionary->release(array->dictionary);
	}
	array->release = nullptr;
	delete holder;
}

//===--------------------------------------------------------------------===//
// Finalize Arrow Child
//===--------------------------------------------------------------------===//
ArrowArray *ArrowAppender::FinalizeChild(const LogicalType &type, unique_ptr<ArrowAppendData> append_data_p) {
	auto result = make_uniq<ArrowArray>();

	auto &append_data = *append_data_p;
	result->private_data = append_data_p.release();
	result->release = ReleaseArray;
	result->n_children = 0;
	result->null_count = 0;
	result->offset = 0;
	result->dictionary = nullptr;
	result->buffers = append_data.buffers.data();
	result->null_count = NumericCast<int64_t>(append_data.null_count);
	result->length = NumericCast<int64_t>(append_data.row_count);
	result->buffers[0] = append_data.GetValidityBuffer().data();

	if (append_data.finalize) {
		append_data.finalize(append_data, type, result.get());
	}

	append_data.array = std::move(result);
	return append_data.array.get();
}

//! Returns the underlying arrow array
ArrowArray ArrowAppender::Finalize() {
	D_ASSERT(root_data.size() == types.size());
	auto root_holder = make_uniq<ArrowAppendData>(options);

	ArrowArray result;
	AddChildren(*root_holder, types.size());
	result.children = root_holder->child_pointers.data();
	result.n_children = NumericCast<int64_t>(types.size());

	// Configure root array
	result.length = NumericCast<int64_t>(row_count);
	result.n_buffers = 1;
	result.buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
	result.offset = 0;
	result.null_count = 0; // needs to be 0
	result.dictionary = nullptr;
	root_holder->child_data = std::move(root_data);

	for (idx_t i = 0; i < root_holder->child_data.size(); i++) {
		root_holder->child_arrays[i] = *ArrowAppender::FinalizeChild(types[i], std::move(root_holder->child_data[i]));
	}

	// Release ownership to caller
	result.private_data = root_holder.release();
	result.release = ArrowAppender::ReleaseArray;
	return result;
}

//===--------------------------------------------------------------------===//
// Initialize Arrow Child
//===--------------------------------------------------------------------===//

template <class OP>
static void InitializeAppenderForType(ArrowAppendData &append_data) {
	append_data.initialize = OP::Initialize;
	append_data.append_vector = OP::Append;
	append_data.finalize = OP::Finalize;
}

static void InitializeFunctionPointers(ArrowAppendData &append_data, const LogicalType &type) {
	// handle special logical types
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
		InitializeAppenderForType<ArrowNullData>(append_data);
		break;
	case LogicalTypeId::BOOLEAN:
		InitializeAppenderForType<ArrowBoolData>(append_data);
		break;
	case LogicalTypeId::TINYINT:
		InitializeAppenderForType<ArrowScalarData<int8_t>>(append_data);
		break;
	case LogicalTypeId::SMALLINT:
		InitializeAppenderForType<ArrowScalarData<int16_t>>(append_data);
		break;
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		InitializeAppenderForType<ArrowScalarData<int32_t>>(append_data);
		break;
	case LogicalTypeId::TIME_TZ: {
		if (append_data.options.arrow_lossless_conversion) {
			InitializeAppenderForType<ArrowScalarData<int64_t>>(append_data);
		} else {
			InitializeAppenderForType<ArrowScalarData<int64_t, dtime_tz_t, ArrowTimeTzConverter>>(append_data);
		}
		break;
	}
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::BIGINT:
		InitializeAppenderForType<ArrowScalarData<int64_t>>(append_data);
		break;
	case LogicalTypeId::UUID:
		if (append_data.options.arrow_lossless_conversion) {
			InitializeAppenderForType<ArrowScalarData<hugeint_t, hugeint_t, ArrowUUIDBlobConverter>>(append_data);
		} else {
			if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				InitializeAppenderForType<ArrowVarcharData<hugeint_t, ArrowUUIDConverter>>(append_data);
			} else {
				InitializeAppenderForType<ArrowVarcharData<hugeint_t, ArrowUUIDConverter, int32_t>>(append_data);
			}
		}
		break;
	case LogicalTypeId::HUGEINT:
		InitializeAppenderForType<ArrowScalarData<hugeint_t>>(append_data);
		break;
	case LogicalTypeId::UHUGEINT:
		InitializeAppenderForType<ArrowScalarData<uhugeint_t>>(append_data);
		break;
	case LogicalTypeId::UTINYINT:
		InitializeAppenderForType<ArrowScalarData<uint8_t>>(append_data);
		break;
	case LogicalTypeId::USMALLINT:
		InitializeAppenderForType<ArrowScalarData<uint16_t>>(append_data);
		break;
	case LogicalTypeId::UINTEGER:
		InitializeAppenderForType<ArrowScalarData<uint32_t>>(append_data);
		break;
	case LogicalTypeId::UBIGINT:
		InitializeAppenderForType<ArrowScalarData<uint64_t>>(append_data);
		break;
	case LogicalTypeId::FLOAT:
		InitializeAppenderForType<ArrowScalarData<float>>(append_data);
		break;
	case LogicalTypeId::DOUBLE:
		InitializeAppenderForType<ArrowScalarData<double>>(append_data);
		break;
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			InitializeAppenderForType<ArrowScalarData<hugeint_t, int16_t>>(append_data);
			break;
		case PhysicalType::INT32:
			InitializeAppenderForType<ArrowScalarData<hugeint_t, int32_t>>(append_data);
			break;
		case PhysicalType::INT64:
			InitializeAppenderForType<ArrowScalarData<hugeint_t, int64_t>>(append_data);
			break;
		case PhysicalType::INT128:
			InitializeAppenderForType<ArrowScalarData<hugeint_t>>(append_data);
			break;
		default:
			throw InternalException("Unsupported internal decimal type");
		}
		break;
	case LogicalTypeId::VARCHAR:
		if (append_data.options.produce_arrow_string_view) {
			InitializeAppenderForType<ArrowVarcharToStringViewData>(append_data);
		} else {
			if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				InitializeAppenderForType<ArrowVarcharData<>>(append_data);
			} else {
				InitializeAppenderForType<ArrowVarcharData<string_t, ArrowVarcharConverter, int32_t>>(append_data);
			}
		}
		break;
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
	case LogicalTypeId::VARINT:
		if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
			InitializeAppenderForType<ArrowVarcharData<>>(append_data);
		} else {
			InitializeAppenderForType<ArrowVarcharData<string_t, ArrowVarcharConverter, int32_t>>(append_data);
		}
		break;
	case LogicalTypeId::ENUM:
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			InitializeAppenderForType<ArrowEnumData<int8_t>>(append_data);
			break;
		case PhysicalType::UINT16:
			InitializeAppenderForType<ArrowEnumData<int16_t>>(append_data);
			break;
		case PhysicalType::UINT32:
			InitializeAppenderForType<ArrowEnumData<int32_t>>(append_data);
			break;
		default:
			throw InternalException("Unsupported internal enum type");
		}
		break;
	case LogicalTypeId::INTERVAL:
		InitializeAppenderForType<ArrowScalarData<ArrowInterval, interval_t, ArrowIntervalConverter>>(append_data);
		break;
	case LogicalTypeId::UNION:
		InitializeAppenderForType<ArrowUnionData>(append_data);
		break;
	case LogicalTypeId::STRUCT:
		InitializeAppenderForType<ArrowStructData>(append_data);
		break;
	case LogicalTypeId::ARRAY:
		InitializeAppenderForType<ArrowFixedSizeListData>(append_data);
		break;
	case LogicalTypeId::LIST: {
		if (append_data.options.arrow_use_list_view) {
			if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				InitializeAppenderForType<ArrowListViewData<>>(append_data);
			} else {
				InitializeAppenderForType<ArrowListViewData<int32_t>>(append_data);
			}
		} else {
			if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				InitializeAppenderForType<ArrowListData<>>(append_data);
			} else {
				InitializeAppenderForType<ArrowListData<int32_t>>(append_data);
			}
		}
		break;
	}
	case LogicalTypeId::MAP:
		// Arrow MapArray only supports 32-bit offsets. There is no LargeMapArray type in Arrow.
		InitializeAppenderForType<ArrowMapData<int32_t>>(append_data);
		break;
	default:
		throw NotImplementedException("Unsupported type in DuckDB -> Arrow Conversion: %s\n", type.ToString());
	}
}

unique_ptr<ArrowAppendData> ArrowAppender::InitializeChild(const LogicalType &type, const idx_t capacity,
                                                           ClientProperties &options,
                                                           const shared_ptr<ArrowTypeExtensionData> &extension_type) {
	auto result = make_uniq<ArrowAppendData>(options);
	LogicalType array_type = type;
	if (extension_type) {
		array_type = extension_type->GetInternalType();
	}
	InitializeFunctionPointers(*result, array_type);
	result->extension_data = extension_type;

	const auto byte_count = (capacity + 7) / 8;
	result->GetValidityBuffer().reserve(byte_count);
	result->initialize(*result, array_type, capacity);
	return result;
}

void ArrowAppender::AddChildren(ArrowAppendData &data, const idx_t count) {
	data.child_pointers.resize(count);
	data.child_arrays.resize(count);
	for (idx_t i = 0; i < count; i++) {
		data.child_pointers[i] = &data.child_arrays[i];
	}
}

} // namespace duckdb
