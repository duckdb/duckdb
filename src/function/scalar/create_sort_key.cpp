#include "duckdb/function/create_sort_key.hpp"

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

namespace {
struct SortKeyBindData : public FunctionData {
	vector<OrderModifiers> modifiers;

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SortKeyBindData>();
		return modifiers == other.modifiers;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<SortKeyBindData>();
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
	auto result = make_uniq<SortKeyBindData>();
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
			bound_function.SetReturnType(LogicalType::BIGINT);
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
		} else {
			format.physical_type = input.GetType().InternalType();
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

	static idx_t Decode(const_data_ptr_t input, Vector &result, TYPE &result_value, bool flip_bytes) {
		if (flip_bytes) {
			// descending order - so flip bytes
			data_t flipped_bytes[sizeof(T)];
			for (idx_t b = 0; b < sizeof(T); b++) {
				flipped_bytes[b] = ~input[b];
			}
			result_value = Radix::DecodeData<T>(flipped_bytes);
		} else {
			result_value = Radix::DecodeData<T>(input);
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

	static idx_t Decode(const_data_ptr_t input, Vector &result, TYPE &result_value, bool flip_bytes) {
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
		result_value = StringVector::EmptyString(result, str_len);
		auto str_data = data_ptr_cast(result_value.GetDataWriteable());
		for (pos = 0; pos < str_len; pos++) {
			if (flip_bytes) {
				str_data[pos] = (~input[pos]) - 1;
			} else {
				str_data[pos] = input[pos] - 1;
			}
		}
		result_value.Finalize();
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

	static idx_t Decode(const_data_ptr_t input, Vector &result, TYPE &result_value, bool flip_bytes) {
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
		result_value = StringVector::EmptyString(result, blob_len);
		auto str_data = data_ptr_cast(result_value.GetDataWriteable());
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
		result_value.Finalize();
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

	inline idx_t GetResultIndex(idx_t r) const {
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

void GetSortKeyLengthRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
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

void GetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result, SortKeyChunk chunk) {
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

void GetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result) {
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

template <class OP, bool ALL_VALID, bool HAS_RESULT_INDEX_OR_SEL>
void TemplatedConstructSortKeyInternal(const SortKeyVectorData &vector_data, const SortKeyChunk chunk,
                                       const SortKeyConstructInfo &info) {
	auto data = UnifiedVectorFormat::GetData<typename OP::TYPE>(vector_data.format);
	auto &offsets = info.offsets;
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		const auto result_index = HAS_RESULT_INDEX_OR_SEL ? chunk.GetResultIndex(r) : r;
		const auto idx = HAS_RESULT_INDEX_OR_SEL ? vector_data.format.sel->get_index(r) : r;
		auto &offset = offsets[result_index];
		auto result_ptr = info.result_data[result_index];
		if (!ALL_VALID && !vector_data.format.validity.RowIsValid(idx)) {
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

template <class OP>
void TemplatedConstructSortKey(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	if (chunk.start == chunk.end) {
		return;
	}
	if (vector_data.format.validity.AllValid()) {
		if (!chunk.has_result_index && !vector_data.format.sel->IsSet()) {
			TemplatedConstructSortKeyInternal<OP, true, false>(vector_data, chunk, info);
		} else {
			TemplatedConstructSortKeyInternal<OP, true, true>(vector_data, chunk, info);
		}
	} else {
		if (!chunk.has_result_index && !vector_data.format.sel->IsSet()) {
			TemplatedConstructSortKeyInternal<OP, false, false>(vector_data, chunk, info);
		} else {
			TemplatedConstructSortKeyInternal<OP, false, true>(vector_data, chunk, info);
		}
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

void ConstructSortKeyRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
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

void ConstructSortKey(SortKeyVectorData &vector_data, SortKeyConstructInfo &info) {
	ConstructSortKeyRecursive(vector_data, SortKeyChunk(0, vector_data.size), info);
}

void PrepareSortData(Vector &result, idx_t size, SortKeyLengthInfo &key_lengths, data_ptr_t *data_pointers) {
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

void FinalizeSortData(Vector &result, idx_t size, const SortKeyLengthInfo &key_lengths,
                      const unsafe_vector<idx_t> &offsets) {
	switch (result.GetType().id()) {
	case LogicalTypeId::BLOB: {
		auto result_data = FlatVector::GetData<string_t>(result);
		// call Finalize on the result
		for (idx_t r = 0; r < size; r++) {
			result_data[r].SetSizeAndFinalize(NumericCast<uint32_t>(offsets[r]),
			                                  key_lengths.variable_lengths[r] + key_lengths.constant_length);
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t r = 0; r < size; r++) {
			result_data[r] = BSwapIfLE(result_data[r]);
		}
		break;
	}
	default:
		throw InternalException("Unsupported key type for CreateSortKey");
	}
}

void CreateSortKeyInternal(vector<unique_ptr<SortKeyVectorData>> &sort_key_data,
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
	FinalizeSortData(result, row_count, key_lengths, offsets);
}

} // namespace

void CreateSortKeyHelpers::CreateSortKey(Vector &input, idx_t input_count, OrderModifiers order_modifier,
                                         Vector &result) {
	// prepare the sort key data
	vector<OrderModifiers> modifiers {order_modifier};
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	sort_key_data.push_back(make_uniq<SortKeyVectorData>(input, input_count, order_modifier));

	CreateSortKeyInternal(sort_key_data, modifiers, result, input_count);
}

void CreateSortKeyHelpers::CreateSortKey(DataChunk &input, const vector<OrderModifiers> &modifiers, Vector &result) {
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	D_ASSERT(modifiers.size() == input.ColumnCount());
	for (idx_t r = 0; r < modifiers.size(); r++) {
		sort_key_data.push_back(make_uniq<SortKeyVectorData>(input.data[r], input.size(), modifiers[r]));
	}
	CreateSortKeyInternal(sort_key_data, modifiers, result, input.size());
}

void CreateSortKeyHelpers::CreateSortKeyWithValidity(Vector &input, Vector &result, const OrderModifiers &modifiers,
                                                     const idx_t count) {
	CreateSortKey(input, count, modifiers, result);
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);
	auto &validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			validity.SetInvalid(i);
		}
	}
}

static void CreateSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<SortKeyBindData>();

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
namespace {

unique_ptr<FunctionData> DecodeSortKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	if ((arguments.size() - 1) % 2 != 0) {
		throw BinderException(
		    "Arguments to decode_sort_key must be [sort_key, col1, sort_specifier1, col2, sort_specifier2, ...]");
	}
	bool all_constant = true;
	idx_t constant_size = 0;
	child_list_t<LogicalType> children;
	auto result = make_uniq<SortKeyBindData>();
	for (idx_t i = 1; i < arguments.size(); i += 2) {
		// Parse column definition
		const auto &col_arg = *arguments[i];
		if (!col_arg.IsFoldable()) {
			throw BinderException("col must be a constant value - but got %s", col_arg.ToString());
		}
		Value col = ExpressionExecutor::EvaluateScalar(context, col_arg);
		const auto col_list = Parser::ParseColumnList(col.ToString());
		if (col_list.LogicalColumnCount() != 1) {
			throw BinderException("decode_sort_key col must contain exactly one column");
		}
		const auto &col_def = col_list.GetColumn(PhysicalIndex(0));
		const auto &col_name = col_def.GetName();
		const auto &col_type = TransformStringToLogicalType(col_def.GetType().ToString(), context);

		// Keep track of this to validate the arguments
		const auto physical_type = col_type.InternalType();
		if (!TypeIsConstantSize(physical_type)) {
			all_constant = false;
		} else {
			// we always add one byte for the validity
			constant_size += GetTypeIdSize(physical_type) + 1;
		}

		// Add name/type to create a struct
		children.emplace_back(col_name, col_type);

		// Parse sort specifier
		const auto &specifier_arg = *arguments[i + 1];
		if (!specifier_arg.IsFoldable()) {
			throw BinderException("sort_specifier must be a constant value - but got %s", specifier_arg.ToString());
		}
		Value sort_specifier = ExpressionExecutor::EvaluateScalar(context, specifier_arg);
		if (sort_specifier.IsNull()) {
			throw BinderException("sort_specifier cannot be NULL");
		}
		const auto sort_specifier_str = sort_specifier.ToString();
		result->modifiers.push_back(OrderModifiers::Parse(sort_specifier_str));
	}

	const auto &sort_key_arg = *arguments[0];
	if (sort_key_arg.return_type == LogicalType::BIGINT) {
		if (!all_constant || constant_size > sizeof(int64_t)) {
			throw BinderException("sort_key has type BIGINT but arguments require BLOB");
		}
	} else if (sort_key_arg.return_type == LogicalType::BLOB) {
		if (all_constant && constant_size <= sizeof(int64_t)) {
			throw BinderException("sort_key has type BLOB but arguments require BIGINT");
		}
	} else {
		throw BinderException("sort_key must be either BIGINT or BLOB, got %s instead",
		                      sort_key_arg.return_type.ToString());
	}
	bound_function.SetReturnType(LogicalType::STRUCT(std::move(children)));

	return std::move(result);
}

struct DecodeSortKeyVectorData {
	DecodeSortKeyVectorData(const LogicalType &type, OrderModifiers modifiers)
	    : flip_bytes(modifiers.order_type == OrderType::DESCENDING) {
		null_byte = SortKeyVectorData::NULL_FIRST_BYTE;
		valid_byte = SortKeyVectorData::NULL_LAST_BYTE;
		if (modifiers.null_type == OrderByNullType::NULLS_LAST) {
			std::swap(null_byte, valid_byte);
		}

		// NULLS FIRST/NULLS LAST passed in by the user are only respected at the top level
		// within nested types NULLS LAST/NULLS FIRST is dependent on ASC/DESC order instead
		// don't blame me this is what Postgres does
		auto child_null_type =
		    modifiers.order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		OrderModifiers child_modifiers(modifiers.order_type, child_null_type);
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			auto &children = StructType::GetChildTypes(type);
			for (auto &child_type : children) {
				child_data.emplace_back(child_type.second, child_modifiers);
			}
			break;
		}
		case PhysicalType::ARRAY: {
			auto &child_type = ArrayType::GetChildType(type);
			child_data.emplace_back(child_type, child_modifiers);
			break;
		}
		case PhysicalType::LIST: {
			auto &child_type = ListType::GetChildType(type);
			child_data.emplace_back(child_type, child_modifiers);
			break;
		}
		default:
			break;
		}
	}

	data_t null_byte;
	data_t valid_byte;
	vector<DecodeSortKeyVectorData> child_data;
	bool flip_bytes;
};

struct DecodeSortKeyData {
#ifdef DEBUG
	DecodeSortKeyData() : data(nullptr), size(DConstants::INVALID_INDEX), position(DConstants::INVALID_INDEX) {
	}
#else
	DecodeSortKeyData() {
	}
#endif

	explicit DecodeSortKeyData(const string_t &sort_key)
	    : data(const_data_ptr_cast(sort_key.GetData())), size(sort_key.GetSize()), position(0) {
	}

	explicit DecodeSortKeyData(const int64_t &sort_key)
	    : data(const_data_ptr_cast(&sort_key)), size(sizeof(int64_t)), position(0) {
	}

	const_data_ptr_t data;
	idx_t size;
	idx_t position;
};

void DecodeSortKeyRecursive(DecodeSortKeyData decode_data[], DecodeSortKeyVectorData &vector_data, Vector &result,
                            idx_t result_offset, idx_t count);

template <class OP>
void TemplatedDecodeSortKey(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                            const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::Validity(result);
	const auto result_data =
	    is_const ? ConstantVector::GetData<typename OP::TYPE>(result) : FlatVector::GetData<typename OP::TYPE>(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		auto validity_byte = decode_data.data[decode_data.position];
		decode_data.position++;
		if (validity_byte == vector_data.null_byte) {
			// NULL value
			result_validity.SetInvalid(result_idx);
			continue;
		}
		idx_t increment = OP::Decode(decode_data.data + decode_data.position, result, result_data[result_idx],
		                             vector_data.flip_bytes);
		decode_data.position += increment;
	}
}

void DecodeSortKeyStruct(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                         const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		// check if the top-level is valid or not
		auto validity_byte = decode_data.data[decode_data.position];
		decode_data.position++;
		if (validity_byte == vector_data.null_byte) {
			// entire struct is NULL
			// note that we still deserialize the children
			result_validity.SetInvalid(result_idx);
		}
	}
	// recurse into children
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t c = 0; c < child_entries.size(); c++) {
		auto &child_entry = child_entries[c];
		DecodeSortKeyRecursive(decode_data_arr, vector_data.child_data[c], *child_entry, result_offset, count);
	}
}

void DecodeSortKeyList(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                       const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::Validity(result);
	const auto list_data =
	    is_const ? ConstantVector::GetData<list_entry_t>(result) : FlatVector::GetData<list_entry_t>(result);
	auto &child_vector = ListVector::GetEntry(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		// check if the top-level is valid or not
		auto validity_byte = decode_data.data[decode_data.position];
		decode_data.position++;
		if (validity_byte == vector_data.null_byte) {
			// entire list is NULL
			result_validity.SetInvalid(result_idx);
			continue;
		}
		// list is valid - decode child elements
		// we don't know how many there will be
		// decode child elements until we encounter the list delimiter
		auto list_delimiter = SortKeyVectorData::LIST_DELIMITER;
		if (vector_data.flip_bytes) {
			list_delimiter = ~list_delimiter;
		}

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
			DecodeSortKeyRecursive(&decode_data, vector_data.child_data[0], child_vector, new_list_size - 1, 1);
		}
		// skip the list delimiter
		decode_data.position++;
		// set the list_entry_t information and update the list size
		list_data[result_idx].length = new_list_size - start_list_size;
		list_data[result_idx].offset = start_list_size;
		ListVector::SetListSize(result, new_list_size);
	}
}

void DecodeSortKeyArray(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                        const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		// check if the top-level is valid or not
		auto validity_byte = decode_data.data[decode_data.position];
		decode_data.position++;
		if (validity_byte == vector_data.null_byte) {
			// entire array is NULL
			// note that we still read the child elements
			result_validity.SetInvalid(result_idx);
		}
		// array is valid - decode child elements
		// arrays need to encode exactly array_size child elements
		// however the decoded data still contains a list delimiter
		// we use this delimiter to verify we successfully decoded the entire array
		auto list_delimiter = SortKeyVectorData::LIST_DELIMITER;
		if (vector_data.flip_bytes) {
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
			DecodeSortKeyRecursive(&decode_data, vector_data.child_data[0], child_vector,
			                       child_start + found_elements - 1, 1);
		}
		// skip the list delimiter
		decode_data.position++;
		if (found_elements != array_size) {
			throw InvalidInputException("Failed to decode array - found %d elements but expected %d", found_elements,
			                            array_size);
		}
	}
}

void DecodeSortKeyRecursive(DecodeSortKeyData decode_data[], DecodeSortKeyVectorData &vector_data, Vector &result,
                            const idx_t result_offset, const idx_t count) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedDecodeSortKey<SortKeyConstantOperator<bool>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::UINT8:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint8_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::INT8:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int8_t>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::UINT16:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint16_t>>(decode_data, vector_data, result, result_offset,
		                                                          count);
		break;
	case PhysicalType::INT16:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int16_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::UINT32:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint32_t>>(decode_data, vector_data, result, result_offset,
		                                                          count);
		break;
	case PhysicalType::INT32:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int32_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::UINT64:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint64_t>>(decode_data, vector_data, result, result_offset,
		                                                          count);
		break;
	case PhysicalType::INT64:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int64_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::FLOAT:
		TemplatedDecodeSortKey<SortKeyConstantOperator<float>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDecodeSortKey<SortKeyConstantOperator<double>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDecodeSortKey<SortKeyConstantOperator<interval_t>>(decode_data, vector_data, result, result_offset,
		                                                            count);
		break;
	case PhysicalType::UINT128:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uhugeint_t>>(decode_data, vector_data, result, result_offset,
		                                                            count);
		break;
	case PhysicalType::INT128:
		TemplatedDecodeSortKey<SortKeyConstantOperator<hugeint_t>>(decode_data, vector_data, result, result_offset,
		                                                           count);
		break;
	case PhysicalType::VARCHAR:
		if (result.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedDecodeSortKey<SortKeyVarcharOperator>(decode_data, vector_data, result, result_offset, count);
		} else {
			TemplatedDecodeSortKey<SortKeyBlobOperator>(decode_data, vector_data, result, result_offset, count);
		}
		break;
	case PhysicalType::STRUCT:
		DecodeSortKeyStruct(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::LIST:
		DecodeSortKeyList(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::ARRAY:
		DecodeSortKeyArray(decode_data, vector_data, result, result_offset, count);
		break;
	default:
		throw NotImplementedException("Unsupported type %s in DecodeSortKey", result.GetType());
	}
}

} // namespace

idx_t CreateSortKeyHelpers::DecodeSortKey(string_t sort_key, Vector &result, idx_t result_idx,
                                          OrderModifiers modifiers) {
	DecodeSortKeyVectorData sort_key_data(result.GetType(), modifiers);
	DecodeSortKeyData decode_data(sort_key);
	DecodeSortKeyRecursive(&decode_data, sort_key_data, result, result_idx, 1);

	return decode_data.position;
}

void CreateSortKeyHelpers::DecodeSortKey(string_t sort_key, DataChunk &result, idx_t result_idx,
                                         const vector<OrderModifiers> &modifiers) {
	DecodeSortKeyData decode_data(sort_key);
	D_ASSERT(modifiers.size() == result.ColumnCount());
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		auto &vec = result.data[c];
		DecodeSortKeyVectorData vector_data(vec.GetType(), modifiers[c]);
		DecodeSortKeyRecursive(&decode_data, vector_data, vec, result_idx, 1);
	}
}

static void DecodeSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<SortKeyBindData>();

	const auto count = args.size();
	auto &sort_key_vec = args.data[0];
	UnifiedVectorFormat sort_key_vec_format;
	sort_key_vec.ToUnifiedFormat(count, sort_key_vec_format);

	// When doing aggressive vector verification, the "sort_key_vec_format.validity.AllValid()" is not always true
	// However, all the actual values should be valid, so we assert that

	// Construct utility for all sort keys that we will decode
	DecodeSortKeyData decode_data[STANDARD_VECTOR_SIZE];
	int64_t bswapped_ints[STANDARD_VECTOR_SIZE];
	if (sort_key_vec.GetType() == LogicalType::BLOB) {
		const auto sort_keys = UnifiedVectorFormat::GetData<string_t>(sort_key_vec_format);
		if (sort_key_vec_format.sel->IsSet()) {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = sort_key_vec_format.sel->get_index(i);
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(idx));
				decode_data[i] = DecodeSortKeyData(sort_keys[idx]);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(i));
				decode_data[i] = DecodeSortKeyData(sort_keys[i]);
			}
		}
	} else {
		D_ASSERT(sort_key_vec.GetType() == LogicalType::BIGINT);
		const auto sort_keys = UnifiedVectorFormat::GetData<int64_t>(sort_key_vec_format);
		if (sort_key_vec_format.sel->IsSet()) {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = sort_key_vec_format.sel->get_index(i);
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(idx));
				bswapped_ints[i] = BSwapIfLE(sort_keys[idx]);
				decode_data[i] = DecodeSortKeyData(bswapped_ints[i]);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(i));
				bswapped_ints[i] = BSwapIfLE(sort_keys[i]);
				decode_data[i] = DecodeSortKeyData(bswapped_ints[i]);
			}
		}
	}

	// Loop through the columns
	const auto &result_type = result.GetType();
	const auto &child_vectors = StructVector::GetEntries(result);
	for (idx_t c = 0; c < StructType::GetChildCount(result_type); c++) {
		auto &child_vector = *child_vectors[c];
		DecodeSortKeyVectorData sort_key_data(child_vector.GetType(), bind_data.modifiers[c]);
		DecodeSortKeyRecursive(decode_data, sort_key_data, child_vector, 0, count);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//===--------------------------------------------------------------------===//
// Get Functions
//===--------------------------------------------------------------------===//
ScalarFunction CreateSortKeyFun::GetFunction() {
	ScalarFunction sort_key_function("create_sort_key", {LogicalType::ANY}, LogicalType::BLOB, CreateSortKeyFunction,
	                                 CreateSortKeyBind);
	sort_key_function.varargs = LogicalType::ANY;
	sort_key_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return sort_key_function;
}

ScalarFunction DecodeSortKeyFun::GetFunction() {
	ScalarFunction sort_key_function("decode_sort_key", {LogicalType::ANY, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::STRUCT({{"any", LogicalType::ANY}}), DecodeSortKeyFunction,
	                                 DecodeSortKeyBind);
	sort_key_function.varargs = LogicalType::VARCHAR;
	return sort_key_function;
}

} // namespace duckdb
