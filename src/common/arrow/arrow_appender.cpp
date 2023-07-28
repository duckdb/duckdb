#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_buffer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/appender/list.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ArrowAppender
//===--------------------------------------------------------------------===//

ArrowAppender::ArrowAppender(vector<LogicalType> types_p, idx_t initial_capacity, ClientProperties options)
    : types(std::move(types_p)) {
	for (auto &type : types) {
		auto entry = ArrowAppender::InitializeChild(type, initial_capacity, options);
		root_data.push_back(std::move(entry));
	}
}

ArrowAppender::~ArrowAppender() {
}

//! Append a data chunk to the underlying arrow array
void ArrowAppender::Append(DataChunk &input, idx_t from, idx_t to, idx_t input_size) {
	D_ASSERT(types == input.GetTypes());
	D_ASSERT(to >= from);
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		root_data[i]->append_vector(*root_data[i], input.data[i], from, to, input_size);
	}
	row_count += to - from;
}

void ArrowAppender::ReleaseArray(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	array->release = nullptr;
	auto holder = static_cast<ArrowAppendData *>(array->private_data);
	delete holder;
}

//===--------------------------------------------------------------------===//
// Finalize Arrow Child
//===--------------------------------------------------------------------===//
ArrowArray *ArrowAppender::FinalizeChild(const LogicalType &type, ArrowAppendData &append_data) {
	auto result = make_uniq<ArrowArray>();

	result->private_data = nullptr;
	result->release = ArrowAppender::ReleaseArray;
	result->n_children = 0;
	result->null_count = 0;
	result->offset = 0;
	result->dictionary = nullptr;
	result->buffers = append_data.buffers.data();
	result->null_count = append_data.null_count;
	result->length = append_data.row_count;
	result->buffers[0] = append_data.validity.data();

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
	root_holder->child_pointers.resize(types.size());
	result.children = root_holder->child_pointers.data();
	result.n_children = types.size();

	// Configure root array
	result.length = row_count;
	result.n_buffers = 1;
	result.buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
	result.offset = 0;
	result.null_count = 0; // needs to be 0
	result.dictionary = nullptr;
	root_holder->child_data = std::move(root_data);

	// FIXME: this violates a property of the arrow format, if root owns all the child memory then consumers can't move
	// child arrays https://arrow.apache.org/docs/format/CDataInterface.html#moving-child-arrays
	for (idx_t i = 0; i < root_holder->child_data.size(); i++) {
		root_holder->child_pointers[i] = ArrowAppender::FinalizeChild(types[i], *root_holder->child_data[i]);
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
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::BIGINT:
		InitializeAppenderForType<ArrowScalarData<int64_t>>(append_data);
		break;
	case LogicalTypeId::HUGEINT:
		InitializeAppenderForType<ArrowScalarData<hugeint_t>>(append_data);
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
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
		if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
			InitializeAppenderForType<ArrowVarcharData<string_t>>(append_data);
		} else {
			InitializeAppenderForType<ArrowVarcharData<string_t, ArrowVarcharConverter, uint32_t>>(append_data);
		}
		break;
	case LogicalTypeId::UUID:
		if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
			InitializeAppenderForType<ArrowVarcharData<hugeint_t, ArrowUUIDConverter>>(append_data);
		} else {
			InitializeAppenderForType<ArrowVarcharData<hugeint_t, ArrowUUIDConverter, uint32_t>>(append_data);
		}
		break;
	case LogicalTypeId::ENUM:
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			InitializeAppenderForType<ArrowEnumData<uint8_t>>(append_data);
			break;
		case PhysicalType::UINT16:
			InitializeAppenderForType<ArrowEnumData<uint16_t>>(append_data);
			break;
		case PhysicalType::UINT32:
			InitializeAppenderForType<ArrowEnumData<uint32_t>>(append_data);
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
	case LogicalTypeId::LIST:
		InitializeAppenderForType<ArrowListData>(append_data);
		break;
	case LogicalTypeId::MAP:
		InitializeAppenderForType<ArrowMapData>(append_data);
		break;
	default:
		throw NotImplementedException("Unsupported type in DuckDB -> Arrow Conversion: %s\n", type.ToString());
	}
}

unique_ptr<ArrowAppendData> ArrowAppender::InitializeChild(const LogicalType &type, idx_t capacity,
                                                           ClientProperties &options) {
	auto result = make_uniq<ArrowAppendData>(options);
	InitializeFunctionPointers(*result, type);

	auto byte_count = (capacity + 7) / 8;
	result->validity.reserve(byte_count);
	result->initialize(*result, type, capacity);
	return result;
}

} // namespace duckdb
