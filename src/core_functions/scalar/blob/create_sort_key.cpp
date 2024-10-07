#include "duckdb/core_functions/scalar/blob_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/core_functions/create_sort_key.hpp"

namespace duckdb {

struct CreateSortKeyBindData : public FunctionData {
	vector<OrderModifiers> modifiers;

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CreateSortKeyBindData>();
		return modifiers == other.modifiers;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<CreateSortKeyBindData>();
		result->modifiers = modifiers;
		return std::move(result);
	}
};

unique_ptr<FunctionData> CreateSortKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() % 2 != 0) {
		throw BinderException(
		    "Arguments to create_sort_key must be [key1, sort_specifier1, key2, sort_specifier2, ...]");
	}
	auto result = make_uniq<CreateSortKeyBindData>();
	for (idx_t i = 1; i < arguments.size(); i += 2) {
		if (!arguments[i]->IsFoldable()) {
			throw BinderException("sort_specifier must be a constant value - but got %s", arguments[i]->ToString());
		}

		// Rebind to return a date if we are truncating that far
		Value sort_specifier = ExpressionExecutor::EvaluateScalar(context, *arguments[i]);
		if (sort_specifier.IsNull()) {
			throw BinderException("sort_specifier cannot be NULL");
		}
		auto sort_specifier_str = sort_specifier.ToString();
		result->modifiers.push_back(OrderModifiers::Parse(sort_specifier_str));
	}
	// push collations
	for (idx_t i = 0; i < arguments.size(); i += 2) {
		ExpressionBinder::PushCollation(context, arguments[i], arguments[i]->return_type);
	}
	// check if all types are constant
	bool all_constant = true;
	idx_t constant_size = 0;
	for (idx_t i = 0; i < arguments.size(); i += 2) {
		auto physical_type = arguments[i]->return_type.InternalType();
		if (!TypeIsConstantSize(physical_type)) {
			all_constant = false;
		} else {
			// we always add one byte for the validity
			constant_size += GetTypeIdSize(physical_type) + 1;
		}
	}
	if (all_constant) {
		if (constant_size <= sizeof(int64_t)) {
			bound_function.return_type = LogicalType::BIGINT;
		}
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Operators
//===--------------------------------------------------------------------===//
struct SortKeyVectorData {
	static constexpr data_t NULL_FIRST_BYTE = 1;
	static constexpr data_t NULL_LAST_BYTE = 2;
	static constexpr data_t STRING_DELIMITER = 0;
	static constexpr data_t LIST_DELIMITER = 0;
	static constexpr data_t BLOB_ESCAPE_CHARACTER = 1;

	SortKeyVectorData(Vector &input, idx_t size, OrderModifiers modifiers) : vec(input) {
		if (size != 0) {
			input.ToUnifiedFormat(size, format);
		}
		this->size = size;

		null_byte = NULL_FIRST_BYTE;
		valid_byte = NULL_LAST_BYTE;
		if (modifiers.null_type == OrderByNullType::NULLS_LAST) {
			std::swap(null_byte, valid_byte);
		}

		// NULLS FIRST/NULLS LAST passed in by the user are only respected at the top level
		// within nested types NULLS LAST/NULLS FIRST is dependent on ASC/DESC order instead
		// don't blame me this is what Postgres does
		auto child_null_type =
		    modifiers.order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		OrderModifiers child_modifiers(modifiers.order_type, child_null_type);
		switch (input.GetType().InternalType()) {
		case PhysicalType::STRUCT: {
			auto &children = StructVector::GetEntries(input);
			for (auto &child : children) {
				child_data.push_back(make_uniq<SortKeyVectorData>(*child, size, child_modifiers));
			}
			break;
		}
		case PhysicalType::ARRAY: {
			auto &child_entry = ArrayVector::GetEntry(input);
			auto array_size = ArrayType::GetSize(input.GetType());
			child_data.push_back(make_uniq<SortKeyVectorData>(child_entry, size * array_size, child_modifiers));
			break;
		}
		case PhysicalType::LIST: {
			auto &child_entry = ListVector::GetEntry(input);
			auto child_size = size == 0 ? 0 : ListVector::GetListSize(input);
			child_data.push_back(make_uniq<SortKeyVectorData>(child_entry, child_size, child_modifiers));
			break;
		}
		default:
			break;
		}
	}
	// disable copy constructors
	SortKeyVectorData(const SortKeyVectorData &other) = delete;
	SortKeyVectorData &operator=(const SortKeyVectorData &) = delete;

	void Initialize() {
	}

	PhysicalType GetPhysicalType() {
		return vec.GetType().InternalType();
	}

	Vector &vec;
	idx_t size;
	UnifiedVectorFormat format;
	vector<unique_ptr<SortKeyVectorData>> child_data;
	data_t null_byte;
	data_t valid_byte;
};

template <class T>
struct SortKeyConstantOperator {
	using TYPE = T;

	static idx_t GetEncodeLength(TYPE input) {
		return sizeof(T);
	}

	static idx_t Encode(data_ptr_t result, TYPE input) {
		Radix::EncodeData<T>(result, input);
		return sizeof(T);
	}

	static idx_t Decode(const_data_ptr_t input, Vector &result, idx_t result_idx, bool flip_bytes) {
		auto result_data = FlatVector::GetData<TYPE>(result);
		if (flip_bytes) {
			// descending order - so flip bytes
			data_t flipped_bytes[sizeof(T)];
			for (idx_t b = 0; b < sizeof(T); b++) {
				flipped_bytes[b] = ~input[b];
			}
			result_data[result_idx] = Radix::DecodeData<T>(flipped_bytes);
		} else {
			result_data[result_idx] = Radix::DecodeData<T>(input);
		}
		return sizeof(T);
	}
};

struct SortKeyVarcharOperator {
	using TYPE = string_t;

	static idx_t GetEncodeLength(TYPE input) {
		return input.GetSize() + 1;
	}

	static idx_t Encode(data_ptr_t result, TYPE input) {
		auto input_data = const_data_ptr_cast(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		for (idx_t r = 0; r < input_size; r++) {
			result[r] = input_data[r] + 1;
		}
		result[input_size] = SortKeyVectorData::STRING_DELIMITER; // null-byte delimiter
		return input_size + 1;
	}

	static idx_t Decode(const_data_ptr_t input, Vector &result, idx_t result_idx, bool flip_bytes) {
		auto result_data = FlatVector::GetData<TYPE>(result);
		// iterate until we encounter the string delimiter to figure out the string length
		data_t string_delimiter = SortKeyVectorData::STRING_DELIMITER;
		if (flip_bytes) {
			string_delimiter = ~string_delimiter;
		}
		idx_t pos;
		for (pos = 0; input[pos] != string_delimiter; pos++) {
		}
		idx_t str_len = pos;
		// now allocate the string data and fill it with the decoded data
		result_data[result_idx] = StringVector::EmptyString(result, str_len);
		auto str_data = data_ptr_cast(result_data[result_idx].GetDataWriteable());
		for (pos = 0; pos < str_len; pos++) {
			if (flip_bytes) {
				str_data[pos] = (~input[pos]) - 1;
			} else {
				str_data[pos] = input[pos] - 1;
			}
		}
		result_data[result_idx].Finalize();
		return pos + 1;
	}
};

struct SortKeyBlobOperator {
	using TYPE = string_t;

	static idx_t GetEncodeLength(TYPE input) {
		auto input_data = data_ptr_t(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		idx_t escaped_characters = 0;
		for (idx_t r = 0; r < input_size; r++) {
			if (input_data[r] <= 1) {
				// we escape both \x00 and \x01
				escaped_characters++;
			}
		}
		return input.GetSize() + escaped_characters + 1;
	}

	static idx_t Encode(data_ptr_t result, TYPE input) {
		auto input_data = data_ptr_t(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		idx_t result_offset = 0;
		for (idx_t r = 0; r < input_size; r++) {
			if (input_data[r] <= 1) {
				// we escape both \x00 and \x01 with \x01
				result[result_offset++] = SortKeyVectorData::BLOB_ESCAPE_CHARACTER;
				result[result_offset++] = input_data[r];
			} else {
				result[result_offset++] = input_data[r];
			}
		}
		result[result_offset++] = SortKeyVectorData::STRING_DELIMITER; // null-byte delimiter
		return result_offset;
	}

	static idx_t Decode(const_data_ptr_t input, Vector &result, idx_t result_idx, bool flip_bytes) {
		auto result_data = FlatVector::GetData<TYPE>(result);
		// scan until we find the delimiter, keeping in mind escapes
		data_t string_delimiter = SortKeyVectorData::STRING_DELIMITER;
		data_t escape_character = SortKeyVectorData::BLOB_ESCAPE_CHARACTER;
		if (flip_bytes) {
			string_delimiter = ~string_delimiter;
			escape_character = ~escape_character;
		}
		idx_t blob_len = 0;
		idx_t pos;
		for (pos = 0; input[pos] != string_delimiter; pos++) {
			blob_len++;
			if (input[pos] == escape_character) {
				// escape character - skip the next byte
				pos++;
			}
		}
		// now allocate the blob data and fill it with the decoded data
		result_data[result_idx] = StringVector::EmptyString(result, blob_len);
		auto str_data = data_ptr_cast(result_data[result_idx].GetDataWriteable());
		for (idx_t input_pos = 0, result_pos = 0; input_pos < pos; input_pos++) {
			if (input[input_pos] == escape_character) {
				// if we encounter an escape character - copy the NEXT byte
				input_pos++;
			}
			if (flip_bytes) {
				str_data[result_pos++] = ~input[input_pos];
			} else {
				str_data[result_pos++] = input[input_pos];
			}
		}
		result_data[result_idx].Finalize();
		return pos + 1;
	}
};

struct SortKeyListEntry {
	static bool IsArray() {
		return false;
	}

	static list_entry_t GetListEntry(SortKeyVectorData &vector_data, idx_t idx) {
		auto data = UnifiedVectorFormat::GetData<list_entry_t>(vector_data.format);
		return data[idx];
	}
};

struct SortKeyArrayEntry {
	static bool IsArray() {
		return true;
	}

	static list_entry_t GetListEntry(SortKeyVectorData &vector_data, idx_t idx) {
		auto array_size = ArrayType::GetSize(vector_data.vec.GetType());
		return list_entry_t(array_size * idx, array_size);
	}
};

struct SortKeyChunk {
	SortKeyChunk(idx_t start, idx_t end) : start(start), end(end), has_result_index(false) {
	}
	SortKeyChunk(idx_t start, idx_t end, idx_t result_index)
	    : start(start), end(end), result_index(result_index), has_result_index(true) {
	}

	idx_t start;
	idx_t end;
	idx_t result_index;
	bool has_result_index;

	inline idx_t GetResultIndex(idx_t r) {
		return has_result_index ? result_index : r;
	}
};

//===--------------------------------------------------------------------===//
// Get Sort Key Length
//===--------------------------------------------------------------------===//
struct SortKeyLengthInfo {
	explicit SortKeyLengthInfo(idx_t size) : constant_length(0) {
		variable_lengths.resize(size, 0);
	}

	idx_t constant_length;
	unsafe_vector<idx_t> variable_lengths;
};

static void GetSortKeyLengthRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result);

template <class OP>
void TemplatedGetSortKeyLength(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	auto &format = vector_data.format;
	auto data = UnifiedVectorFormat::GetData<typename OP::TYPE>(vector_data.format);
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto idx = format.sel->get_index(r);
		auto result_index = chunk.GetResultIndex(r);
		result.variable_lengths[result_index]++; // every value is prefixed by a validity byte

		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		result.variable_lengths[result_index] += OP::GetEncodeLength(data[idx]);
	}
}

void GetSortKeyLengthStruct(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto result_index = chunk.GetResultIndex(r);
		result.variable_lengths[result_index]++; // every struct is prefixed by a validity byte
	}
	// now recursively call GetSortKeyLength on the child elements
	for (auto &child_data : vector_data.child_data) {
		GetSortKeyLengthRecursive(*child_data, chunk, result);
	}
}

template <class OP>
void GetSortKeyLengthList(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	auto &child_data = vector_data.child_data[0];
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto idx = vector_data.format.sel->get_index(r);
		auto result_index = chunk.GetResultIndex(r);
		result.variable_lengths[result_index]++; // every list is prefixed by a validity byte

		if (!vector_data.format.validity.RowIsValid(idx)) {
			if (!OP::IsArray()) {
				// for arrays we need to fill in the child vector for all elements, even if the top-level array is NULL
				continue;
			}
		}
		auto list_entry = OP::GetListEntry(vector_data, idx);
		// for each non-null list we have an "end of list" delimiter
		result.variable_lengths[result_index]++;
		if (list_entry.length > 0) {
			// recursively call GetSortKeyLength for the children of this list
			SortKeyChunk child_chunk(list_entry.offset, list_entry.offset + list_entry.length, result_index);
			GetSortKeyLengthRecursive(*child_data, child_chunk, result);
		}
	}
}

static void GetSortKeyLengthRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	auto physical_type = vector_data.GetPhysicalType();
	// handle variable lengths
	switch (physical_type) {
	case PhysicalType::BOOL:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<bool>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT8:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint8_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT8:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int8_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT16:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint16_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT16:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int16_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT32:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint32_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT32:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int32_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT64:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint64_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT64:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int64_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<float>>(vector_data, chunk, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<double>>(vector_data, chunk, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<interval_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT128:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uhugeint_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT128:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<hugeint_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::VARCHAR:
		if (vector_data.vec.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedGetSortKeyLength<SortKeyVarcharOperator>(vector_data, chunk, result);
		} else {
			TemplatedGetSortKeyLength<SortKeyBlobOperator>(vector_data, chunk, result);
		}
		break;
	case PhysicalType::STRUCT:
		GetSortKeyLengthStruct(vector_data, chunk, result);
		break;
	case PhysicalType::LIST:
		GetSortKeyLengthList<SortKeyListEntry>(vector_data, chunk, result);
		break;
	case PhysicalType::ARRAY:
		GetSortKeyLengthList<SortKeyArrayEntry>(vector_data, chunk, result);
		break;
	default:
		throw NotImplementedException("Unsupported physical type %s in GetSortKeyLength", physical_type);
	}
}

static void GetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result, SortKeyChunk chunk) {
	// top-level method
	auto physical_type = vector_data.GetPhysicalType();
	if (TypeIsConstantSize(physical_type)) {
		// every row is prefixed by a validity byte
		result.constant_length += 1;
		result.constant_length += GetTypeIdSize(physical_type);
		return;
	}
	GetSortKeyLengthRecursive(vector_data, chunk, result);
}

static void GetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result) {
	GetSortKeyLength(vector_data, result, SortKeyChunk(0, vector_data.size));
}

//===--------------------------------------------------------------------===//
// Construct Sort Key
//===--------------------------------------------------------------------===//
struct SortKeyConstructInfo {
	SortKeyConstructInfo(OrderModifiers modifiers_p, unsafe_vector<idx_t> &offsets, data_ptr_t *result_data)
	    : modifiers(modifiers_p), offsets(offsets), result_data(result_data) {
		flip_bytes = modifiers.order_type == OrderType::DESCENDING;
	}

	OrderModifiers modifiers;
	unsafe_vector<idx_t> &offsets;
	data_ptr_t *result_data;
	bool flip_bytes;
};

static void ConstructSortKeyRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info);

template <class OP>
void TemplatedConstructSortKey(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	auto data = UnifiedVectorFormat::GetData<typename OP::TYPE>(vector_data.format);
	auto &offsets = info.offsets;
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto result_index = chunk.GetResultIndex(r);
		auto idx = vector_data.format.sel->get_index(r);
		auto &offset = offsets[result_index];
		auto result_ptr = info.result_data[result_index];
		if (!vector_data.format.validity.RowIsValid(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
			continue;
		}
		// valid value - write the validity byte
		result_ptr[offset++] = vector_data.valid_byte;
		idx_t encode_len = OP::Encode(result_ptr + offset, data[idx]);
		if (info.flip_bytes) {
			// descending order - so flip bytes
			for (idx_t b = offset; b < offset + encode_len; b++) {
				result_ptr[b] = ~result_ptr[b];
			}
		}
		offset += encode_len;
	}
}

void ConstructSortKeyStruct(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	bool list_of_structs = chunk.has_result_index;
	// write the validity data of the struct
	auto &offsets = info.offsets;
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto result_index = chunk.GetResultIndex(r);
		auto idx = vector_data.format.sel->get_index(r);
		auto &offset = offsets[result_index];
		auto result_ptr = info.result_data[result_index];
		if (!vector_data.format.validity.RowIsValid(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
		} else {
			// valid value - write the validity byte
			result_ptr[offset++] = vector_data.valid_byte;
		}
		if (list_of_structs) {
			// for a list of structs we need to write the child data for every iteration
			// since the final layout needs to be
			// [struct1][struct2][...]
			for (auto &child : vector_data.child_data) {
				SortKeyChunk child_chunk(r, r + 1, result_index);
				ConstructSortKeyRecursive(*child, child_chunk, info);
			}
		}
	}
	if (!list_of_structs) {
		for (auto &child : vector_data.child_data) {
			ConstructSortKeyRecursive(*child, chunk, info);
		}
	}
}

template <class OP>
void ConstructSortKeyList(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	auto &offsets = info.offsets;
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto result_index = chunk.GetResultIndex(r);
		auto idx = vector_data.format.sel->get_index(r);
		auto &offset = offsets[result_index];
		auto result_ptr = info.result_data[result_index];
		if (!vector_data.format.validity.RowIsValid(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
			if (!OP::IsArray()) {
				// for arrays we always write the child elements - also if the top-level array is NULL
				continue;
			}
		} else {
			// valid value - write the validity byte
			result_ptr[offset++] = vector_data.valid_byte;
		}

		auto list_entry = OP::GetListEntry(vector_data, idx);
		// recurse and write the list elements
		if (list_entry.length > 0) {
			SortKeyChunk child_chunk(list_entry.offset, list_entry.offset + list_entry.length, result_index);
			ConstructSortKeyRecursive(*vector_data.child_data[0], child_chunk, info);
		}

		// write the end-of-list delimiter
		result_ptr[offset++] = static_cast<data_t>(info.flip_bytes ? ~SortKeyVectorData::LIST_DELIMITER
		                                                           : SortKeyVectorData::LIST_DELIMITER);
	}
}

static void ConstructSortKeyRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	switch (vector_data.GetPhysicalType()) {
	case PhysicalType::BOOL:
		TemplatedConstructSortKey<SortKeyConstantOperator<bool>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT8:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint8_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT8:
		TemplatedConstructSortKey<SortKeyConstantOperator<int8_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT16:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint16_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT16:
		TemplatedConstructSortKey<SortKeyConstantOperator<int16_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT32:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint32_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT32:
		TemplatedConstructSortKey<SortKeyConstantOperator<int32_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT64:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint64_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT64:
		TemplatedConstructSortKey<SortKeyConstantOperator<int64_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::FLOAT:
		TemplatedConstructSortKey<SortKeyConstantOperator<float>>(vector_data, chunk, info);
		break;
	case PhysicalType::DOUBLE:
		TemplatedConstructSortKey<SortKeyConstantOperator<double>>(vector_data, chunk, info);
		break;
	case PhysicalType::INTERVAL:
		TemplatedConstructSortKey<SortKeyConstantOperator<interval_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT128:
		TemplatedConstructSortKey<SortKeyConstantOperator<uhugeint_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT128:
		TemplatedConstructSortKey<SortKeyConstantOperator<hugeint_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::VARCHAR:
		if (vector_data.vec.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedConstructSortKey<SortKeyVarcharOperator>(vector_data, chunk, info);
		} else {
			TemplatedConstructSortKey<SortKeyBlobOperator>(vector_data, chunk, info);
		}
		break;
	case PhysicalType::STRUCT:
		ConstructSortKeyStruct(vector_data, chunk, info);
		break;
	case PhysicalType::LIST:
		ConstructSortKeyList<SortKeyListEntry>(vector_data, chunk, info);
		break;
	case PhysicalType::ARRAY:
		ConstructSortKeyList<SortKeyArrayEntry>(vector_data, chunk, info);
		break;
	default:
		throw NotImplementedException("Unsupported type %s in ConstructSortKey", vector_data.vec.GetType());
	}
}

static void ConstructSortKey(SortKeyVectorData &vector_data, SortKeyConstructInfo &info) {
	ConstructSortKeyRecursive(vector_data, SortKeyChunk(0, vector_data.size), info);
}

static void PrepareSortData(Vector &result, idx_t size, SortKeyLengthInfo &key_lengths, data_ptr_t *data_pointers) {
	switch (result.GetType().id()) {
	case LogicalTypeId::BLOB: {
		auto result_data = FlatVector::GetData<string_t>(result);
		for (idx_t r = 0; r < size; r++) {
			auto blob_size = key_lengths.variable_lengths[r] + key_lengths.constant_length;
			result_data[r] = StringVector::EmptyString(result, blob_size);
			data_pointers[r] = data_ptr_cast(result_data[r].GetDataWriteable());
#ifdef DEBUG
			memset(data_pointers[r], 0xFF, blob_size);
#endif
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t r = 0; r < size; r++) {
			result_data[r] = 0;
			data_pointers[r] = data_ptr_cast(&result_data[r]);
		}
		break;
	}
	default:
		throw InternalException("Unsupported key type for CreateSortKey");
	}
}

static void FinalizeSortData(Vector &result, idx_t size) {
	switch (result.GetType().id()) {
	case LogicalTypeId::BLOB: {
		auto result_data = FlatVector::GetData<string_t>(result);
		// call Finalize on the result
		for (idx_t r = 0; r < size; r++) {
			result_data[r].Finalize();
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t r = 0; r < size; r++) {
			result_data[r] = BSwap(result_data[r]);
		}
		break;
	}
	default:
		throw InternalException("Unsupported key type for CreateSortKey");
	}
}

static void CreateSortKeyInternal(vector<unique_ptr<SortKeyVectorData>> &sort_key_data,
                                  const vector<OrderModifiers> &modifiers, Vector &result, idx_t row_count) {
	// two phases
	// a) get the length of the final sorted key
	// b) allocate the sorted key and construct
	// we do all of this in a vectorized manner
	SortKeyLengthInfo key_lengths(row_count);
	for (auto &vector_data : sort_key_data) {
		GetSortKeyLength(*vector_data, key_lengths);
	}
	// allocate the empty sort keys
	auto data_pointers = unique_ptr<data_ptr_t[]>(new data_ptr_t[row_count]);
	PrepareSortData(result, row_count, key_lengths, data_pointers.get());

	unsafe_vector<idx_t> offsets;
	offsets.resize(row_count, 0);
	// now construct the sort keys
	for (idx_t c = 0; c < sort_key_data.size(); c++) {
		SortKeyConstructInfo info(modifiers[c], offsets, data_pointers.get());
		ConstructSortKey(*sort_key_data[c], info);
	}
	FinalizeSortData(result, row_count);
}

void CreateSortKeyHelpers::CreateSortKey(Vector &input, idx_t input_count, OrderModifiers order_modifier,
                                         Vector &result) {
	// prepare the sort key data
	vector<OrderModifiers> modifiers {order_modifier};
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	sort_key_data.push_back(make_uniq<SortKeyVectorData>(input, input_count, order_modifier));

	CreateSortKeyInternal(sort_key_data, modifiers, result, input_count);
}

static void CreateSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<CreateSortKeyBindData>();

	// prepare the sort key data
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	for (idx_t c = 0; c < args.ColumnCount(); c += 2) {
		sort_key_data.push_back(make_uniq<SortKeyVectorData>(args.data[c], args.size(), bind_data.modifiers[c / 2]));
	}
	CreateSortKeyInternal(sort_key_data, bind_data.modifiers, result, args.size());

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//===--------------------------------------------------------------------===//
// Decode Sort Key
//===--------------------------------------------------------------------===//
struct DecodeSortKeyData {
	explicit DecodeSortKeyData(OrderModifiers modifiers, string_t &sort_key)
	    : data(const_data_ptr_cast(sort_key.GetData())), size(sort_key.GetSize()), position(0),
	      flip_bytes(modifiers.order_type == OrderType::DESCENDING) {
	}

	const_data_ptr_t data;
	idx_t size;
	idx_t position;
	bool flip_bytes;
};

void DecodeSortKeyRecursive(DecodeSortKeyData &decode_data, SortKeyVectorData &vector_data, Vector &result,
                            idx_t result_idx);

template <class OP>
void TemplatedDecodeSortKey(DecodeSortKeyData &decode_data, SortKeyVectorData &vector_data, Vector &result,
                            idx_t result_idx) {
	auto validity_byte = decode_data.data[decode_data.position];
	decode_data.position++;
	if (validity_byte == vector_data.null_byte) {
		// NULL value
		FlatVector::Validity(result).SetInvalid(result_idx);
		return;
	}
	idx_t increment = OP::Decode(decode_data.data + decode_data.position, result, result_idx, decode_data.flip_bytes);
	decode_data.position += increment;
}

void DecodeSortKeyStruct(DecodeSortKeyData &decode_data, SortKeyVectorData &vector_data, Vector &result,
                         idx_t result_idx) {
	// check if the top-level is valid or not
	auto validity_byte = decode_data.data[decode_data.position];
	decode_data.position++;
	if (validity_byte == vector_data.null_byte) {
		// entire struct is NULL
		// note that we still deserialize the children
		FlatVector::Validity(result).SetInvalid(result_idx);
	}
	// recurse into children
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t c = 0; c < child_entries.size(); c++) {
		auto &child_entry = child_entries[c];
		DecodeSortKeyRecursive(decode_data, *vector_data.child_data[c], *child_entry, result_idx);
	}
}

void DecodeSortKeyList(DecodeSortKeyData &decode_data, SortKeyVectorData &vector_data, Vector &result,
                       idx_t result_idx) {
	// check if the top-level is valid or not
	auto validity_byte = decode_data.data[decode_data.position];
	decode_data.position++;
	if (validity_byte == vector_data.null_byte) {
		// entire list is NULL
		FlatVector::Validity(result).SetInvalid(result_idx);
		return;
	}
	// list is valid - decode child elements
	// we don't know how many there will be
	// decode child elements until we encounter the list delimiter
	auto list_delimiter = SortKeyVectorData::LIST_DELIMITER;
	if (decode_data.flip_bytes) {
		list_delimiter = ~list_delimiter;
	}
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &child_vector = ListVector::GetEntry(result);
	// get the current list size
	auto start_list_size = ListVector::GetListSize(result);
	auto new_list_size = start_list_size;
	// loop until we find the list delimiter
	while (decode_data.data[decode_data.position] != list_delimiter) {
		// found a valid entry here - decode it
		// first reserve space for it
		new_list_size++;
		ListVector::Reserve(result, new_list_size);

		// now decode the entry
		DecodeSortKeyRecursive(decode_data, *vector_data.child_data[0], child_vector, new_list_size - 1);
	}
	// skip the list delimiter
	decode_data.position++;
	// set the list_entry_t information and update the list size
	list_data[result_idx].length = new_list_size - start_list_size;
	list_data[result_idx].offset = start_list_size;
	ListVector::SetListSize(result, new_list_size);
}

void DecodeSortKeyArray(DecodeSortKeyData &decode_data, SortKeyVectorData &vector_data, Vector &result,
                        idx_t result_idx) {
	// check if the top-level is valid or not
	auto validity_byte = decode_data.data[decode_data.position];
	decode_data.position++;
	if (validity_byte == vector_data.null_byte) {
		// entire array is NULL
		// note that we still read the child elements
		FlatVector::Validity(result).SetInvalid(result_idx);
	}
	// array is valid - decode child elements
	// arrays need to encode exactly array_size child elements
	// however the decoded data still contains a list delimiter
	// we use this delimiter to verify we successfully decoded the entire array
	auto list_delimiter = SortKeyVectorData::LIST_DELIMITER;
	if (decode_data.flip_bytes) {
		list_delimiter = ~list_delimiter;
	}
	auto &child_vector = ArrayVector::GetEntry(result);
	auto array_size = ArrayType::GetSize(result.GetType());

	idx_t found_elements = 0;
	auto child_start = array_size * result_idx;
	// loop until we find the list delimiter
	while (decode_data.data[decode_data.position] != list_delimiter) {
		found_elements++;
		if (found_elements > array_size) {
			// error - found too many elements
			break;
		}
		// now decode the entry
		DecodeSortKeyRecursive(decode_data, *vector_data.child_data[0], child_vector, child_start + found_elements - 1);
	}
	// skip the list delimiter
	decode_data.position++;
	if (found_elements != array_size) {
		throw InvalidInputException("Failed to decode array - found %d elements but expected %d", found_elements,
		                            array_size);
	}
}

void DecodeSortKeyRecursive(DecodeSortKeyData &decode_data, SortKeyVectorData &vector_data, Vector &result,
                            idx_t result_idx) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedDecodeSortKey<SortKeyConstantOperator<bool>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::UINT8:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint8_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::INT8:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int8_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::UINT16:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint16_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::INT16:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int16_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::UINT32:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint32_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::INT32:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int32_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::UINT64:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint64_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::INT64:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int64_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::FLOAT:
		TemplatedDecodeSortKey<SortKeyConstantOperator<float>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDecodeSortKey<SortKeyConstantOperator<double>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDecodeSortKey<SortKeyConstantOperator<interval_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::UINT128:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uhugeint_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::INT128:
		TemplatedDecodeSortKey<SortKeyConstantOperator<hugeint_t>>(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::VARCHAR:
		if (vector_data.vec.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedDecodeSortKey<SortKeyVarcharOperator>(decode_data, vector_data, result, result_idx);
		} else {
			TemplatedDecodeSortKey<SortKeyBlobOperator>(decode_data, vector_data, result, result_idx);
		}
		break;
	case PhysicalType::STRUCT:
		DecodeSortKeyStruct(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::LIST:
		DecodeSortKeyList(decode_data, vector_data, result, result_idx);
		break;
	case PhysicalType::ARRAY:
		DecodeSortKeyArray(decode_data, vector_data, result, result_idx);
		break;
	default:
		throw NotImplementedException("Unsupported type %s in DecodeSortKey", vector_data.vec.GetType());
	}
}

void CreateSortKeyHelpers::DecodeSortKey(string_t sort_key, Vector &result, idx_t result_idx,
                                         OrderModifiers modifiers) {
	SortKeyVectorData sort_key_data(result, 0, modifiers);
	DecodeSortKeyData decode_data(modifiers, sort_key);
	DecodeSortKeyRecursive(decode_data, sort_key_data, result, result_idx);
}

ScalarFunction CreateSortKeyFun::GetFunction() {
	ScalarFunction sort_key_function("create_sort_key", {LogicalType::ANY}, LogicalType::BLOB, CreateSortKeyFunction,
	                                 CreateSortKeyBind);
	sort_key_function.varargs = LogicalType::ANY;
	sort_key_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return sort_key_function;
}

} // namespace duckdb
