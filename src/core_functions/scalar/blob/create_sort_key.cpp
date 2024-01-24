#include "duckdb/core_functions/scalar/blob_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

struct OrderModifiers {
	OrderModifiers(OrderType order_type, OrderByNullType null_type) : order_type(order_type), null_type(null_type) {
	}

	OrderType order_type;
	OrderByNullType null_type;

	bool operator==(const OrderModifiers &other) const {
		return order_type == other.order_type && null_type == other.null_type;
	}

	static OrderModifiers Parse(const string &val) {
		auto lcase = StringUtil::Replace(StringUtil::Lower(val), "_", " ");
		OrderType order_type;
		if (StringUtil::StartsWith(lcase, "asc")) {
			order_type = OrderType::ASCENDING;
		} else if (StringUtil::StartsWith(lcase, "desc")) {
			order_type = OrderType::DESCENDING;
		} else {
			throw BinderException("create_sort_key modifier must start with either ASC or DESC");
		}
		OrderByNullType null_type;
		if (StringUtil::EndsWith(lcase, "nulls first")) {
			null_type = OrderByNullType::NULLS_FIRST;
		} else if (StringUtil::EndsWith(lcase, "nulls last")) {
			null_type = OrderByNullType::NULLS_LAST;
		} else {
			throw BinderException("create_sort_key modifier must end with either NULLS FIRST or NULLS LAST");
		}
		return OrderModifiers(order_type, null_type);
	}
};

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
		ExpressionBinder::PushCollation(context, arguments[i], arguments[i]->return_type, false);
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
		input.ToUnifiedFormat(size, format);
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
			auto child_size = ListVector::GetListSize(input);
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
};

struct SortKeyVarcharOperator {
	using TYPE = string_t;

	static idx_t GetEncodeLength(TYPE input) {
		return input.GetSize() + 1;
	}

	static idx_t Encode(data_ptr_t result, TYPE input) {
		auto input_data = input.GetDataUnsafe();
		auto input_size = input.GetSize();
		for (idx_t r = 0; r < input_size; r++) {
			result[r] = input_data[r] + 1;
		}
		result[input_size] = SortKeyVectorData::STRING_DELIMITER; // null-byte delimiter
		return input_size + 1;
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

static void GetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result) {
	// top-level method
	auto physical_type = vector_data.GetPhysicalType();
	if (TypeIsConstantSize(physical_type)) {
		// every row is prefixed by a validity byte
		result.constant_length += 1;
		result.constant_length += GetTypeIdSize(physical_type);
		return;
	}
	GetSortKeyLengthRecursive(vector_data, SortKeyChunk(0, vector_data.size), result);
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
		result_ptr[offset++] = info.flip_bytes ? ~SortKeyVectorData::LIST_DELIMITER : SortKeyVectorData::LIST_DELIMITER;
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
			result_data[r] = BSwap<int64_t>(result_data[r]);
		}
		break;
	}
	default:
		throw InternalException("Unsupported key type for CreateSortKey");
	}
}

static void CreateSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<CreateSortKeyBindData>();

	// prepare the sort key data
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	for (idx_t c = 0; c < args.ColumnCount(); c += 2) {
		sort_key_data.push_back(make_uniq<SortKeyVectorData>(args.data[c], args.size(), bind_data.modifiers[c / 2]));
	}

	// two phases
	// a) get the length of the final sorted key
	// b) allocate the sorted key and construct
	// we do all of this in a vectorized manner
	SortKeyLengthInfo key_lengths(args.size());
	for (auto &vector_data : sort_key_data) {
		GetSortKeyLength(*vector_data, key_lengths);
	}
	// allocate the empty sort keys
	auto data_pointers = unique_ptr<data_ptr_t[]>(new data_ptr_t[args.size()]);
	PrepareSortData(result, args.size(), key_lengths, data_pointers.get());

	unsafe_vector<idx_t> offsets;
	offsets.resize(args.size(), 0);
	// now construct the sort keys
	for (idx_t c = 0; c < sort_key_data.size(); c++) {
		SortKeyConstructInfo info(bind_data.modifiers[c], offsets, data_pointers.get());
		ConstructSortKey(*sort_key_data[c], info);
	}
	FinalizeSortData(result, args.size());
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction CreateSortKeyFun::GetFunction() {
	ScalarFunction sort_key_function({LogicalType::ANY}, LogicalType::BLOB, CreateSortKeyFunction, CreateSortKeyBind);
	sort_key_function.varargs = LogicalType::ANY;
	sort_key_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return sort_key_function;
}

} // namespace duckdb
