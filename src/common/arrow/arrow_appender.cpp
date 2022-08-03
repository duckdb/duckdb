#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/array.hpp"

namespace duckdb {

struct ArrowBuffer {
	ArrowBuffer() : dataptr(nullptr), count(0), capacity(0) {
	}
	~ArrowBuffer() {
		if (!dataptr) {
			return;
		}
		free(dataptr);
		dataptr = nullptr;
		count = 0;
		capacity = 0;
	}
	// disable copy constructors
	ArrowBuffer(const ArrowBuffer &other) = delete;
	ArrowBuffer &operator=(const ArrowBuffer &) = delete;
	//! enable move constructors
	ArrowBuffer(ArrowBuffer &&other) noexcept {
		std::swap(dataptr, other.dataptr);
		std::swap(count, other.count);
		std::swap(capacity, other.capacity);
	}
	ArrowBuffer &operator=(ArrowBuffer &&other) noexcept {
		std::swap(dataptr, other.dataptr);
		std::swap(count, other.count);
		std::swap(capacity, other.capacity);
		return *this;
	}

	void reserve(idx_t bytes) {
		auto new_capacity = NextPowerOfTwo(bytes);
		if (new_capacity <= capacity) {
			return;
		}
		reserve_internal(new_capacity);
	}

	void resize(idx_t bytes) {
		reserve(bytes);
		count = bytes;
	}

	idx_t size() {
		return count;
	}

	data_ptr_t data() {
		return dataptr;
	}

	void shrink_to_fit() {
		reserve_internal(count);
	}

private:
	void reserve_internal(idx_t bytes) {
		if (dataptr) {
			dataptr = (data_ptr_t)realloc(dataptr, bytes);
		} else {
			dataptr = (data_ptr_t)malloc(bytes);
		}
		capacity = bytes;
	}

private:
	data_ptr_t dataptr = nullptr;
	idx_t count = 0;
	idx_t capacity = 0;
};

struct ArrowAppendData {
	// the buffers of the arrow vector
	ArrowBuffer validity;
	ArrowBuffer main_buffer;
	ArrowBuffer aux_buffer;

	idx_t row_count = 0;

	// child data (if any)
	vector<ArrowAppendData> child_data;

	//! the arrow array C API data, only set after Finalize
	unique_ptr<ArrowArray> array;
	duckdb::array<const void *, 3> buffers = {{nullptr, nullptr, nullptr}};
	vector<ArrowArray *> child_pointers;
};

ArrowAppender::ArrowAppender(vector<LogicalType> types_p, idx_t initial_capacity) : types(move(types_p)) {

	for (auto &type : types) {
		auto entry = Initialize(type, initial_capacity);
		root_data.push_back(move(entry));
	}
}

ArrowAppender::~ArrowAppender() {
}

static void GetBitPosition(idx_t row_idx, uint8_t &current_byte, uint8_t &current_bit) {
	current_byte = row_idx / 8;
	current_bit = row_idx % 8;
}

static void SetBit(bool value, uint8_t *data, uint8_t &current_byte, uint8_t &current_bit) {
	if (current_bit == 8) {
		current_byte++;
		current_bit = 0;
	}
	if (!value) {
		//! We set the bit to 0
		data[current_byte] &= ~(1 << current_bit);
	} else {
		//! We set the bit to 1
		data[current_byte] |= 1 << current_bit;
	}
	current_bit++;
}

static void AppendValidity(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t size) {
	auto byte_count = (append_data.row_count + size + 7) / 8;
	append_data.validity.resize(byte_count);

	auto validity_data = (uint8_t *)append_data.validity.data();
	uint8_t current_bit, current_byte;
	GetBitPosition(append_data.row_count, current_byte, current_bit);
	for (idx_t i = 0; i < size; i++) {
		auto source_idx = format.sel->get_index(i);
		// append the validity mask
		SetBit(format.validity.RowIsValid(source_idx), validity_data, current_byte, current_bit);
	}
}

template <class T>
void TemplatedAppendVector(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t size) {
	append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(T) * size);
	auto data = (T *)format.data;
	auto result_data = (T *)append_data.main_buffer.data();

	for (idx_t i = 0; i < size; i++) {
		auto source_idx = format.sel->get_index(i);
		auto result_idx = append_data.row_count + i;

		// append the main data
		result_data[result_idx] = data[source_idx];
	}
	append_data.row_count += size;
}

void AppendBool(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t size) {
	auto byte_count = (append_data.row_count + size + 7) / 8;
	append_data.main_buffer.resize(byte_count);
	auto data = (bool *)format.data;

	auto result_data = (uint8_t *)append_data.main_buffer.data();
	uint8_t current_bit, current_byte;
	GetBitPosition(append_data.row_count, current_byte, current_bit);
	for (idx_t i = 0; i < size; i++) {
		auto source_idx = format.sel->get_index(i);
		// append the validity mask
		SetBit(format.validity.RowIsValid(source_idx) ? data[source_idx] : false, result_data, current_byte,
		       current_bit);
	}
	append_data.row_count += size;
}

void ArrowAppender::AppendVector(ArrowAppendData &append_data, Vector &input, idx_t size) {
	// handle special logical types
	switch (input.GetType().id()) {
	case LogicalTypeId::MAP:
	case LogicalTypeId::UUID:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::ENUM:
		throw InternalException("FIXME: special logical type");
	default:
		break;
	}
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(size, format);

	AppendValidity(append_data, format, size);

	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
		AppendBool(append_data, format, size);
		break;
	case PhysicalType::VARCHAR:
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		throw InternalException("FIXME: special physical type");
	case PhysicalType::INT8:
		TemplatedAppendVector<int8_t>(append_data, format, size);
		break;
	case PhysicalType::INT16:
		TemplatedAppendVector<int16_t>(append_data, format, size);
		break;
	case PhysicalType::INT32:
		TemplatedAppendVector<int32_t>(append_data, format, size);
		break;
	case PhysicalType::INT64:
		TemplatedAppendVector<int64_t>(append_data, format, size);
		break;
	case PhysicalType::UINT8:
		TemplatedAppendVector<uint8_t>(append_data, format, size);
		break;
	case PhysicalType::UINT16:
		TemplatedAppendVector<uint16_t>(append_data, format, size);
		break;
	case PhysicalType::UINT32:
		TemplatedAppendVector<uint32_t>(append_data, format, size);
		break;
	case PhysicalType::UINT64:
		TemplatedAppendVector<uint64_t>(append_data, format, size);
		break;
	case PhysicalType::INT128:
		TemplatedAppendVector<hugeint_t>(append_data, format, size);
		break;
	case PhysicalType::FLOAT:
		TemplatedAppendVector<float>(append_data, format, size);
		break;
	case PhysicalType::DOUBLE:
		TemplatedAppendVector<double>(append_data, format, size);
		break;
	default:
		throw InternalException("FIXME: unsupported physical type");
	}
}

//! Append a data chunk to the underlying arrow array
void ArrowAppender::Append(DataChunk &input) {
	D_ASSERT(types == input.GetTypes());
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		AppendVector(root_data[i], input.data[i], input.size());
	}
	row_count += input.size();
}

static void ReleaseDuckDBArrowAppendArray(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	array->release = nullptr;
	auto holder = static_cast<ArrowAppendData *>(array->private_data);
	delete holder;
}

ArrowArray *ArrowAppender::FinalizeArrowChild(const LogicalType &type, ArrowAppendData &append_data) {
	auto result = make_unique<ArrowArray>();

	result->private_data = nullptr;
	result->release = ReleaseDuckDBArrowAppendArray;
	result->n_children = 0;
	result->null_count = 0;
	result->offset = 0;
	result->dictionary = nullptr;
	result->buffers = append_data.buffers.data();

	result->length = append_data.row_count;

	// the actual initialization depends on the type
	switch (type.id()) {
	case LogicalTypeId::MAP:
	case LogicalTypeId::UUID:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::ENUM:
		throw InternalException("FIXME: arrow finalize");
	default:
		break;
	}

	// FIXME: 0 if there are no nulls
	result->null_count = -1;
	result->buffers[0] = append_data.validity.data();
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR: {
		result->n_buffers = 3;
		result->buffers[1] = append_data.main_buffer.data();
		result->buffers[2] = append_data.aux_buffer.data();
		break;
	}
	case PhysicalType::STRUCT: {
		result->n_buffers = 1;

		auto &child_types = StructType::GetChildTypes(type);
		append_data.child_pointers.resize(child_types.size());
		result->children = append_data.child_pointers.data();
		result->n_children = child_types.size();
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &child_type = child_types[i].second;
			append_data.child_pointers[i] = FinalizeArrowChild(child_type, append_data.child_data[i]);
		}
		break;
	}
	case PhysicalType::LIST: {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();

		auto &child_type = ListType::GetChildType(type);
		append_data.child_pointers.resize(1);
		result->children = append_data.child_pointers.data();
		result->n_children = 1;
		append_data.child_pointers[0] = FinalizeArrowChild(child_type, append_data.child_data[0]);
		break;
	}
	default: {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
		break;
	}
	}

	append_data.array = move(result);
	return append_data.array.get();
}

//! Returns the underlying arrow array
ArrowArray ArrowAppender::Finalize() {
	auto root_holder = make_unique<ArrowAppendData>();

	ArrowArray result;
	root_holder->child_pointers.resize(types.size());
	result.children = root_holder->child_pointers.data();
	result.n_children = types.size();

	// Configure root array
	result.length = row_count;
	result.n_children = types.size();
	result.n_buffers = 1;
	result.buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
	result.offset = 0;
	result.null_count = 0; // needs to be 0
	result.dictionary = nullptr;
	root_holder->child_data = move(root_data);

	for (idx_t i = 0; i < root_holder->child_data.size(); i++) {
		root_holder->child_pointers[i] = FinalizeArrowChild(types[i], root_holder->child_data[i]);
	}

	// Release ownership to caller
	result.private_data = root_holder.release();
	result.release = ReleaseDuckDBArrowAppendArray;
	return result;
}

ArrowAppendData ArrowAppender::Initialize(const LogicalType &type, idx_t capacity) {
	ArrowAppendData result;

	switch (type.id()) {
	case LogicalTypeId::MAP:
	case LogicalTypeId::UUID:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::ENUM:
		throw InternalException("FIXME: arrow appender");
	default:
		break;
	}

	auto byte_count = (capacity + 7) / 8;
	result.validity.reserve(byte_count);
	switch (type.InternalType()) {
	case PhysicalType::BOOL: {
		// booleans are stored as bitmasks
		result.main_buffer.reserve(byte_count);
		break;
	}
	case PhysicalType::VARCHAR:
		result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));
		result.aux_buffer.reserve(capacity);
		break;
	case PhysicalType::STRUCT: {
		auto &children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			auto child_buffer = Initialize(child.second, capacity);
			result.child_data.push_back(move(child_buffer));
		}
		break;
	}
	case PhysicalType::LIST: {
		auto &child_type = ListType::GetChildType(type);
		result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));
		auto child_buffer = Initialize(child_type, capacity);
		result.child_data.push_back(move(child_buffer));
		break;
	}
	default:
		result.main_buffer.reserve(capacity * GetTypeIdSize(type.InternalType()));
		break;
	}
	return result;
}

} // namespace duckdb
