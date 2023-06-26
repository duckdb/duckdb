#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_buffer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arrow append data
//===--------------------------------------------------------------------===//
typedef void (*initialize_t)(ArrowAppendData &result, const LogicalType &type, idx_t capacity);
typedef void (*append_vector_t)(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size);
typedef void (*finalize_t)(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result);

struct ArrowAppendData {
	explicit ArrowAppendData(ArrowOptions &options_p) : options(options_p) {
	}
	// the buffers of the arrow vector
	ArrowBuffer validity;
	ArrowBuffer main_buffer;
	ArrowBuffer aux_buffer;

	idx_t row_count = 0;
	idx_t null_count = 0;

	// function pointers for construction
	initialize_t initialize = nullptr;
	append_vector_t append_vector = nullptr;
	finalize_t finalize = nullptr;

	// child data (if any)
	vector<unique_ptr<ArrowAppendData>> child_data;

	// the arrow array C API data, only set after Finalize
	unique_ptr<ArrowArray> array;
	duckdb::array<const void *, 3> buffers = {{nullptr, nullptr, nullptr}};
	vector<ArrowArray *> child_pointers;

	ArrowOptions options;
};

//===--------------------------------------------------------------------===//
// ArrowAppender
//===--------------------------------------------------------------------===//
static unique_ptr<ArrowAppendData> InitializeArrowChild(const LogicalType &type, idx_t capacity, ArrowOptions &options);
static ArrowArray *FinalizeArrowChild(const LogicalType &type, ArrowAppendData &append_data);

ArrowAppender::ArrowAppender(vector<LogicalType> types_p, idx_t initial_capacity, ArrowOptions options)
    : types(std::move(types_p)) {
	for (auto &type : types) {
		auto entry = InitializeArrowChild(type, initial_capacity, options);
		root_data.push_back(std::move(entry));
	}
}

ArrowAppender::~ArrowAppender() {
}

//===--------------------------------------------------------------------===//
// Append Helper Functions
//===--------------------------------------------------------------------===//
static void GetBitPosition(idx_t row_idx, idx_t &current_byte, uint8_t &current_bit) {
	current_byte = row_idx / 8;
	current_bit = row_idx % 8;
}

static void UnsetBit(uint8_t *data, idx_t current_byte, uint8_t current_bit) {
	data[current_byte] &= ~((uint64_t)1 << current_bit);
}

static void NextBit(idx_t &current_byte, uint8_t &current_bit) {
	current_bit++;
	if (current_bit == 8) {
		current_byte++;
		current_bit = 0;
	}
}

static void ResizeValidity(ArrowBuffer &buffer, idx_t row_count) {
	auto byte_count = (row_count + 7) / 8;
	buffer.resize(byte_count, 0xFF);
}

static void SetNull(ArrowAppendData &append_data, uint8_t *validity_data, idx_t current_byte, uint8_t current_bit) {
	UnsetBit(validity_data, current_byte, current_bit);
	append_data.null_count++;
}

static void AppendValidity(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t from, idx_t to) {
	// resize the buffer, filling the validity buffer with all valid values
	idx_t size = to - from;
	ResizeValidity(append_data.validity, append_data.row_count + size);
	if (format.validity.AllValid()) {
		// if all values are valid we don't need to do anything else
		return;
	}

	// otherwise we iterate through the validity mask
	auto validity_data = (uint8_t *)append_data.validity.data();
	uint8_t current_bit;
	idx_t current_byte;
	GetBitPosition(append_data.row_count, current_byte, current_bit);
	for (idx_t i = from; i < to; i++) {
		auto source_idx = format.sel->get_index(i);
		// append the validity mask
		if (!format.validity.RowIsValid(source_idx)) {
			SetNull(append_data, validity_data, current_byte, current_bit);
		}
		NextBit(current_byte, current_bit);
	}
}

//===--------------------------------------------------------------------===//
// Scalar Types
//===--------------------------------------------------------------------===//
struct ArrowScalarConverter {
	template <class TGT, class SRC>
	static TGT Operation(SRC input) {
		return input;
	}

	static bool SkipNulls() {
		return false;
	}

	template <class TGT>
	static void SetNull(TGT &value) {
	}
};

struct ArrowIntervalConverter {
	template <class TGT, class SRC>
	static TGT Operation(SRC input) {
		ArrowInterval result;
		result.months = input.months;
		result.days = input.days;
		result.nanoseconds = input.micros * Interval::NANOS_PER_MICRO;
		return result;
	}

	static bool SkipNulls() {
		return true;
	}

	template <class TGT>
	static void SetNull(TGT &value) {
	}
};

template <class TGT, class SRC = TGT, class OP = ArrowScalarConverter>
struct ArrowScalarBaseData {
	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		D_ASSERT(to >= from);
		idx_t size = to - from;
		D_ASSERT(size <= input_size);
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);

		// append the validity mask
		AppendValidity(append_data, format, from, to);

		// append the main data
		append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(TGT) * size);
		auto data = UnifiedVectorFormat::GetData<SRC>(format);
		auto result_data = append_data.main_buffer.GetData<TGT>();

		for (idx_t i = from; i < to; i++) {
			auto source_idx = format.sel->get_index(i);
			auto result_idx = append_data.row_count + i - from;

			if (OP::SkipNulls() && !format.validity.RowIsValid(source_idx)) {
				OP::template SetNull<TGT>(result_data[result_idx]);
				continue;
			}
			result_data[result_idx] = OP::template Operation<TGT, SRC>(data[source_idx]);
		}
		append_data.row_count += size;
	}
};

template <class TGT, class SRC = TGT, class OP = ArrowScalarConverter>
struct ArrowScalarData : public ArrowScalarBaseData<TGT, SRC, OP> {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve(capacity * sizeof(TGT));
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
	}
};

//===--------------------------------------------------------------------===//
// Enums
//===--------------------------------------------------------------------===//
template <class TGT>
struct ArrowEnumData : public ArrowScalarBaseData<TGT> {
	static idx_t GetLength(string_t input) {
		return input.GetSize();
	}
	static void WriteData(data_ptr_t target, string_t input) {
		memcpy(target, input.GetData(), input.GetSize());
	}
	static void EnumAppendVector(ArrowAppendData &append_data, const Vector &input, idx_t size) {
		D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);

		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(append_data.validity, append_data.row_count + size);

		// resize the offset buffer - the offset buffer holds the offsets into the child array
		append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(uint32_t) * (size + 1));
		auto data = FlatVector::GetData<string_t>(input);
		auto offset_data = append_data.main_buffer.GetData<uint32_t>();
		if (append_data.row_count == 0) {
			// first entry
			offset_data[0] = 0;
		}
		// now append the string data to the auxiliary buffer
		// the auxiliary buffer's length depends on the string lengths, so we resize as required
		auto last_offset = offset_data[append_data.row_count];
		for (idx_t i = 0; i < size; i++) {
			auto offset_idx = append_data.row_count + i + 1;

			auto string_length = GetLength(data[i]);

			// append the offset data
			auto current_offset = last_offset + string_length;
			offset_data[offset_idx] = current_offset;

			// resize the string buffer if required, and write the string data
			append_data.aux_buffer.resize(current_offset);
			WriteData(append_data.aux_buffer.data() + last_offset, data[i]);

			last_offset = current_offset;
		}
		append_data.row_count += size;
	}
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve(capacity * sizeof(TGT));
		// construct the enum child data
		auto enum_data = InitializeArrowChild(LogicalType::VARCHAR, EnumType::GetSize(type), result.options);
		EnumAppendVector(*enum_data, EnumType::GetValuesInsertOrder(type), EnumType::GetSize(type));
		result.child_data.push_back(std::move(enum_data));
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
		// finalize the enum child data, and assign it to the dictionary
		result->dictionary = FinalizeArrowChild(LogicalType::VARCHAR, *append_data.child_data[0]);
	}
};

//===--------------------------------------------------------------------===//
// Boolean
//===--------------------------------------------------------------------===//
struct ArrowBoolData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		auto byte_count = (capacity + 7) / 8;
		result.main_buffer.reserve(byte_count);
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		idx_t size = to - from;
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);

		// we initialize both the validity and the bit set to 1's
		ResizeValidity(append_data.validity, append_data.row_count + size);
		ResizeValidity(append_data.main_buffer, append_data.row_count + size);
		auto data = UnifiedVectorFormat::GetData<bool>(format);

		auto result_data = append_data.main_buffer.GetData<uint8_t>();
		auto validity_data = append_data.validity.GetData<uint8_t>();
		uint8_t current_bit;
		idx_t current_byte;
		GetBitPosition(append_data.row_count, current_byte, current_bit);
		for (idx_t i = from; i < to; i++) {
			auto source_idx = format.sel->get_index(i);
			// append the validity mask
			if (!format.validity.RowIsValid(source_idx)) {
				SetNull(append_data, validity_data, current_byte, current_bit);
			} else if (!data[source_idx]) {
				UnsetBit(result_data, current_byte, current_bit);
			}
			NextBit(current_byte, current_bit);
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
	}
};

//===--------------------------------------------------------------------===//
// Varchar
//===--------------------------------------------------------------------===//
struct ArrowVarcharConverter {
	template <class SRC>
	static idx_t GetLength(SRC input) {
		return input.GetSize();
	}

	template <class SRC>
	static void WriteData(data_ptr_t target, SRC input) {
		memcpy(target, input.GetData(), input.GetSize());
	}
};

struct ArrowUUIDConverter {
	template <class SRC>
	static idx_t GetLength(SRC input) {
		return UUID::STRING_SIZE;
	}

	template <class SRC>
	static void WriteData(data_ptr_t target, SRC input) {
		UUID::ToString(input, char_ptr_cast(target));
	}
};

template <class SRC = string_t, class OP = ArrowVarcharConverter, class BUFTYPE = uint64_t>
struct ArrowVarcharData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve((capacity + 1) * sizeof(BUFTYPE));

		result.aux_buffer.reserve(capacity);
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		idx_t size = to - from;
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);

		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(append_data.validity, append_data.row_count + size);
		auto validity_data = (uint8_t *)append_data.validity.data();

		// resize the offset buffer - the offset buffer holds the offsets into the child array
		append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
		auto data = UnifiedVectorFormat::GetData<SRC>(format);
		auto offset_data = append_data.main_buffer.GetData<BUFTYPE>();
		if (append_data.row_count == 0) {
			// first entry
			offset_data[0] = 0;
		}
		// now append the string data to the auxiliary buffer
		// the auxiliary buffer's length depends on the string lengths, so we resize as required
		auto last_offset = offset_data[append_data.row_count];
		idx_t max_offset = append_data.row_count + to - from;
		if (max_offset > NumericLimits<uint32_t>::Maximum() &&
		    append_data.options.offset_size == ArrowOffsetSize::REGULAR) {
			throw InvalidInputException("Arrow Appender: The maximum total string size for regular string buffers is "
			                            "%u but the offset of %lu exceeds this.",
			                            NumericLimits<uint32_t>::Maximum(), max_offset);
		}
		for (idx_t i = from; i < to; i++) {
			auto source_idx = format.sel->get_index(i);
			auto offset_idx = append_data.row_count + i + 1 - from;

			if (!format.validity.RowIsValid(source_idx)) {
				uint8_t current_bit;
				idx_t current_byte;
				GetBitPosition(append_data.row_count + i - from, current_byte, current_bit);
				SetNull(append_data, validity_data, current_byte, current_bit);
				offset_data[offset_idx] = last_offset;
				continue;
			}

			auto string_length = OP::GetLength(data[source_idx]);

			// append the offset data
			auto current_offset = last_offset + string_length;
			offset_data[offset_idx] = current_offset;

			// resize the string buffer if required, and write the string data
			append_data.aux_buffer.resize(current_offset);
			OP::WriteData(append_data.aux_buffer.data() + last_offset, data[source_idx]);

			last_offset = current_offset;
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 3;
		result->buffers[1] = append_data.main_buffer.data();
		result->buffers[2] = append_data.aux_buffer.data();
	}
};

//===--------------------------------------------------------------------===//
// Structs
//===--------------------------------------------------------------------===//
struct ArrowStructData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		auto &children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			auto child_buffer = InitializeArrowChild(child.second, capacity, result.options);
			result.child_data.push_back(std::move(child_buffer));
		}
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);
		idx_t size = to - from;
		AppendValidity(append_data, format, from, to);
		// append the children of the struct
		auto &children = StructVector::GetEntries(input);
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child = children[child_idx];
			auto &child_data = *append_data.child_data[child_idx];
			child_data.append_vector(child_data, *child, from, to, size);
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 1;

		auto &child_types = StructType::GetChildTypes(type);
		append_data.child_pointers.resize(child_types.size());
		result->children = append_data.child_pointers.data();
		result->n_children = child_types.size();
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &child_type = child_types[i].second;
			append_data.child_pointers[i] = FinalizeArrowChild(child_type, *append_data.child_data[i]);
		}
	}
};

//===--------------------------------------------------------------------===//
// Lists
//===--------------------------------------------------------------------===//
void AppendListOffsets(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t from, idx_t to,
                       vector<sel_t> &child_sel) {
	// resize the offset buffer - the offset buffer holds the offsets into the child array
	idx_t size = to - from;
	append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(uint32_t) * (size + 1));
	auto data = UnifiedVectorFormat::GetData<list_entry_t>(format);
	auto offset_data = append_data.main_buffer.GetData<uint32_t>();
	if (append_data.row_count == 0) {
		// first entry
		offset_data[0] = 0;
	}
	// set up the offsets using the list entries
	auto last_offset = offset_data[append_data.row_count];
	for (idx_t i = from; i < to; i++) {
		auto source_idx = format.sel->get_index(i);
		auto offset_idx = append_data.row_count + i + 1 - from;

		if (!format.validity.RowIsValid(source_idx)) {
			offset_data[offset_idx] = last_offset;
			continue;
		}

		// append the offset data
		auto list_length = data[source_idx].length;
		last_offset += list_length;
		offset_data[offset_idx] = last_offset;

		for (idx_t k = 0; k < list_length; k++) {
			child_sel.push_back(data[source_idx].offset + k);
		}
	}
}

struct ArrowListData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		auto &child_type = ListType::GetChildType(type);
		result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));
		auto child_buffer = InitializeArrowChild(child_type, capacity, result.options);
		result.child_data.push_back(std::move(child_buffer));
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);
		idx_t size = to - from;
		vector<sel_t> child_indices;
		AppendValidity(append_data, format, from, to);
		AppendListOffsets(append_data, format, from, to, child_indices);

		// append the child vector of the list
		SelectionVector child_sel(child_indices.data());
		auto &child = ListVector::GetEntry(input);
		auto child_size = child_indices.size();
		Vector child_copy(child.GetType());
		child_copy.Slice(child, child_sel, child_size);
		append_data.child_data[0]->append_vector(*append_data.child_data[0], child_copy, 0, child_size, child_size);
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();

		auto &child_type = ListType::GetChildType(type);
		append_data.child_pointers.resize(1);
		result->children = append_data.child_pointers.data();
		result->n_children = 1;
		append_data.child_pointers[0] = FinalizeArrowChild(child_type, *append_data.child_data[0]);
	}
};

//===--------------------------------------------------------------------===//
// Maps
//===--------------------------------------------------------------------===//
struct ArrowMapData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		// map types are stored in a (too) clever way
		// the main buffer holds the null values and the offsets
		// then we have a single child, which is a struct of the map_type, and the key_type
		result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));

		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		auto internal_struct = make_uniq<ArrowAppendData>(result.options);
		internal_struct->child_data.push_back(InitializeArrowChild(key_type, capacity, result.options));
		internal_struct->child_data.push_back(InitializeArrowChild(value_type, capacity, result.options));

		result.child_data.push_back(std::move(internal_struct));
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);
		idx_t size = to - from;
		AppendValidity(append_data, format, from, to);
		vector<sel_t> child_indices;
		AppendListOffsets(append_data, format, from, to, child_indices);

		SelectionVector child_sel(child_indices.data());
		auto &key_vector = MapVector::GetKeys(input);
		auto &value_vector = MapVector::GetValues(input);
		auto list_size = child_indices.size();

		auto &struct_data = *append_data.child_data[0];
		auto &key_data = *struct_data.child_data[0];
		auto &value_data = *struct_data.child_data[1];

		if (size != input_size) {
			// Let's avoid doing this
			Vector key_vector_copy(key_vector.GetType());
			key_vector_copy.Slice(key_vector, child_sel, list_size);
			Vector value_vector_copy(value_vector.GetType());
			value_vector_copy.Slice(value_vector, child_sel, list_size);
			key_data.append_vector(key_data, key_vector_copy, 0, list_size, list_size);
			value_data.append_vector(value_data, value_vector_copy, 0, list_size, list_size);
		} else {
			// We don't care about the vector, slice it
			key_vector.Slice(child_sel, list_size);
			value_vector.Slice(child_sel, list_size);
			key_data.append_vector(key_data, key_vector, 0, list_size, list_size);
			value_data.append_vector(value_data, value_vector, 0, list_size, list_size);
		}

		append_data.row_count += size;
		struct_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		// set up the main map buffer
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();

		// the main map buffer has a single child: a struct
		append_data.child_pointers.resize(1);
		result->children = append_data.child_pointers.data();
		result->n_children = 1;
		append_data.child_pointers[0] = FinalizeArrowChild(type, *append_data.child_data[0]);

		// now that struct has two children: the key and the value type
		auto &struct_data = *append_data.child_data[0];
		auto &struct_result = append_data.child_pointers[0];
		struct_data.child_pointers.resize(2);
		struct_result->n_buffers = 1;
		struct_result->n_children = 2;
		struct_result->length = struct_data.child_data[0]->row_count;
		struct_result->children = struct_data.child_pointers.data();

		D_ASSERT(struct_data.child_data[0]->row_count == struct_data.child_data[1]->row_count);

		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		struct_data.child_pointers[0] = FinalizeArrowChild(key_type, *struct_data.child_data[0]);
		struct_data.child_pointers[1] = FinalizeArrowChild(value_type, *struct_data.child_data[1]);

		// keys cannot have null values
		if (struct_data.child_pointers[0]->null_count > 0) {
			throw std::runtime_error("Arrow doesn't accept NULL keys on Maps");
		}
	}
};

//! Append a data chunk to the underlying arrow array
void ArrowAppender::Append(DataChunk &input, idx_t from, idx_t to, idx_t input_size) {
	D_ASSERT(types == input.GetTypes());
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		root_data[i]->append_vector(*root_data[i], input.data[i], from, to, input_size);
	}
	row_count += to - from;
}
//===--------------------------------------------------------------------===//
// Initialize Arrow Child
//===--------------------------------------------------------------------===//
template <class OP>
static void InitializeFunctionPointers(ArrowAppendData &append_data) {
	append_data.initialize = OP::Initialize;
	append_data.append_vector = OP::Append;
	append_data.finalize = OP::Finalize;
}

static void InitializeFunctionPointers(ArrowAppendData &append_data, const LogicalType &type) {
	// handle special logical types
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		InitializeFunctionPointers<ArrowBoolData>(append_data);
		break;
	case LogicalTypeId::TINYINT:
		InitializeFunctionPointers<ArrowScalarData<int8_t>>(append_data);
		break;
	case LogicalTypeId::SMALLINT:
		InitializeFunctionPointers<ArrowScalarData<int16_t>>(append_data);
		break;
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		InitializeFunctionPointers<ArrowScalarData<int32_t>>(append_data);
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::BIGINT:
		InitializeFunctionPointers<ArrowScalarData<int64_t>>(append_data);
		break;
	case LogicalTypeId::HUGEINT:
		InitializeFunctionPointers<ArrowScalarData<hugeint_t>>(append_data);
		break;
	case LogicalTypeId::UTINYINT:
		InitializeFunctionPointers<ArrowScalarData<uint8_t>>(append_data);
		break;
	case LogicalTypeId::USMALLINT:
		InitializeFunctionPointers<ArrowScalarData<uint16_t>>(append_data);
		break;
	case LogicalTypeId::UINTEGER:
		InitializeFunctionPointers<ArrowScalarData<uint32_t>>(append_data);
		break;
	case LogicalTypeId::UBIGINT:
		InitializeFunctionPointers<ArrowScalarData<uint64_t>>(append_data);
		break;
	case LogicalTypeId::FLOAT:
		InitializeFunctionPointers<ArrowScalarData<float>>(append_data);
		break;
	case LogicalTypeId::DOUBLE:
		InitializeFunctionPointers<ArrowScalarData<double>>(append_data);
		break;
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t, int16_t>>(append_data);
			break;
		case PhysicalType::INT32:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t, int32_t>>(append_data);
			break;
		case PhysicalType::INT64:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t, int64_t>>(append_data);
			break;
		case PhysicalType::INT128:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t>>(append_data);
			break;
		default:
			throw InternalException("Unsupported internal decimal type");
		}
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
		if (append_data.options.offset_size == ArrowOffsetSize::LARGE) {
			InitializeFunctionPointers<ArrowVarcharData<string_t>>(append_data);
		} else {
			InitializeFunctionPointers<ArrowVarcharData<string_t, ArrowVarcharConverter, uint32_t>>(append_data);
		}
		break;
	case LogicalTypeId::UUID:
		if (append_data.options.offset_size == ArrowOffsetSize::LARGE) {
			InitializeFunctionPointers<ArrowVarcharData<hugeint_t, ArrowUUIDConverter>>(append_data);
		} else {
			InitializeFunctionPointers<ArrowVarcharData<hugeint_t, ArrowUUIDConverter, uint32_t>>(append_data);
		}
		break;
	case LogicalTypeId::ENUM:
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			InitializeFunctionPointers<ArrowEnumData<uint8_t>>(append_data);
			break;
		case PhysicalType::UINT16:
			InitializeFunctionPointers<ArrowEnumData<uint16_t>>(append_data);
			break;
		case PhysicalType::UINT32:
			InitializeFunctionPointers<ArrowEnumData<uint32_t>>(append_data);
			break;
		default:
			throw InternalException("Unsupported internal enum type");
		}
		break;
	case LogicalTypeId::INTERVAL:
		InitializeFunctionPointers<ArrowScalarData<ArrowInterval, interval_t, ArrowIntervalConverter>>(append_data);
		break;
	case LogicalTypeId::STRUCT:
		InitializeFunctionPointers<ArrowStructData>(append_data);
		break;
	case LogicalTypeId::LIST:
		InitializeFunctionPointers<ArrowListData>(append_data);
		break;
	case LogicalTypeId::MAP:
		InitializeFunctionPointers<ArrowMapData>(append_data);
		break;
	default:
		throw InternalException("Unsupported type in DuckDB -> Arrow Conversion: %s\n", type.ToString());
	}
}

unique_ptr<ArrowAppendData> InitializeArrowChild(const LogicalType &type, idx_t capacity, ArrowOptions &options) {
	auto result = make_uniq<ArrowAppendData>(options);
	InitializeFunctionPointers(*result, type);

	auto byte_count = (capacity + 7) / 8;
	result->validity.reserve(byte_count);
	result->initialize(*result, type, capacity);
	return result;
}

static void ReleaseDuckDBArrowAppendArray(ArrowArray *array) {
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
ArrowArray *FinalizeArrowChild(const LogicalType &type, ArrowAppendData &append_data) {
	auto result = make_uniq<ArrowArray>();

	result->private_data = nullptr;
	result->release = ReleaseDuckDBArrowAppendArray;
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
	result.n_children = types.size();
	result.n_buffers = 1;
	result.buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
	result.offset = 0;
	result.null_count = 0; // needs to be 0
	result.dictionary = nullptr;
	root_holder->child_data = std::move(root_data);

	for (idx_t i = 0; i < root_holder->child_data.size(); i++) {
		root_holder->child_pointers[i] = FinalizeArrowChild(types[i], *root_holder->child_data[i]);
	}

	// Release ownership to caller
	result.private_data = root_holder.release();
	result.release = ReleaseDuckDBArrowAppendArray;
	return result;
}

} // namespace duckdb
