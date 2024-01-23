#include "duckdb/core_functions/scalar/blob_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct OrderModifiers {
	OrderModifiers(OrderType order_type, OrderByNullType null_type) : order_type(order_type), null_type(null_type) {}

	OrderType order_type;
	OrderByNullType null_type;

	bool operator==(const OrderModifiers& other) const {
		return order_type == other.order_type && null_type == other.null_type;
	}

	static OrderModifiers Parse(const string &val) {
		auto lcase = StringUtil::Lower(val);
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
	CreateSortKeyBindData() {}

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
		throw BinderException("Arguments to create_sort_key must be [key1, sort_specifier1, key2, sort_specifier2, ...]");
	}
	auto result = make_uniq<CreateSortKeyBindData>();
	for(idx_t i = 1; i < arguments.size(); i += 2) {
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
	return result;
}

//===--------------------------------------------------------------------===//
// Operators
//===--------------------------------------------------------------------===//
template<class T>
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
		for(idx_t r = 0; r < input_size; r++) {
			result[r] = input_data[r] + 1;
		}
		result[input_size] = 0; // null-byte delimiter
		return input_size + 1;
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

struct SortKeyVectorData {
	SortKeyVectorData(Vector &input, idx_t size, OrderModifiers modifiers) :
		vec(input) {
		input.ToUnifiedFormat(size, format);
		this->size = size;

		null_byte = modifiers.null_type == OrderByNullType::NULLS_LAST ? 1 : 0;
		valid_byte = 1 - null_byte;

		// NULLS FIRST/NULLS LAST passed in by the user are only respected at the top level
		// within nested types NULLS LAST/NULLS FIRST is dependent on ASC/DESC order instead
		// don't blame me this is what Postgres does
		auto child_null_type = modifiers.order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		OrderModifiers child_modifiers(modifiers.order_type, child_null_type);
		switch(input.GetType().InternalType()) {
		case PhysicalType::LIST: {
			auto &child_entry = ListVector::GetEntry(input);
			auto child_size = ListVector::GetListSize(input);
			child_data.emplace_back(child_entry, child_size, child_modifiers);
			break;
		}
		default:
			break;
		}
	}

	PhysicalType GetPhysicalType() {
		return vec.GetType().InternalType();
	}

	Vector &vec;
	idx_t size;
	UnifiedVectorFormat format;
	vector<SortKeyVectorData> child_data;
	SelectionVector result_sel;
	data_t null_byte;
	data_t valid_byte;
};

static void GetSortKeyLengthRecursive(SortKeyVectorData &vector_data, SortKeyLengthInfo &result);

template<class OP>
void TemplatedGetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result) {
	auto &format = vector_data.format;
	auto data = UnifiedVectorFormat::GetData<typename OP::TYPE>(vector_data.format);
	for(idx_t r = 0; r < vector_data.size; r++) {
		auto result_index = vector_data.result_sel.get_index(r);
		result.variable_lengths[result_index]++; // every value is prefixed by a validity byte

		auto idx = format.sel->get_index(r);
		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		result.variable_lengths[result_index] += OP::GetEncodeLength(data[idx]);
	}
}

void GetSortKeyLengthList(SortKeyVectorData &vector_data, SortKeyLengthInfo &result) {
	auto data = UnifiedVectorFormat::GetData<list_entry_t>(vector_data.format);
	auto &child_data = vector_data.child_data[0];
	child_data.result_sel.Initialize(child_data.size);
	for (idx_t r = 0; r < vector_data.size; r++) {
		auto result_index = vector_data.result_sel.get_index(r);
		result.variable_lengths[result_index]++; // every list is prefixed by a validity byte

		auto idx = vector_data.format.sel->get_index(r);
		if (!vector_data.format.validity.RowIsValid(idx)) {
			continue;
		}
		auto list_entry = data[idx];
		// for each non-null list we have an "end of list" delimiter
		result.variable_lengths[result_index]++;
		// set up the selection vector for the child
		for (idx_t k = 0; k < list_entry.length; k++) {
			child_data.result_sel.set_index(list_entry.offset + k, result_index);
		}
	}
	// now recursively call GetSortKeyLength on the child element
	GetSortKeyLengthRecursive(child_data, result);
}

static void GetSortKeyLengthRecursive(SortKeyVectorData &vector_data, SortKeyLengthInfo &result) {
	auto physical_type = vector_data.GetPhysicalType();
	// handle variable lengths
	switch(physical_type) {
	case PhysicalType::INT32:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int32_t>>(vector_data, result);
		break;
	case PhysicalType::VARCHAR:
		if (vector_data.vec.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedGetSortKeyLength<SortKeyVarcharOperator>(vector_data, result);
		} else {
			throw NotImplementedException("FIXME: ConstructSortKey blob");
		}
		break;
	case PhysicalType::LIST:
		GetSortKeyLengthList(vector_data, result);
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
	vector_data.result_sel.Initialize(*FlatVector::IncrementalSelectionVector());
	GetSortKeyLengthRecursive(vector_data, result);
}

//===--------------------------------------------------------------------===//
// Construct Sort Key
//===--------------------------------------------------------------------===//
struct SortKeyConstructInfo {
	SortKeyConstructInfo(OrderModifiers modifiers_p, unsafe_vector<idx_t> &offsets, string_t *result_data) :
		modifiers(modifiers_p), offsets(offsets), result_data(result_data) {
		flip_bytes = modifiers.order_type == OrderType::DESCENDING;
	}

	OrderModifiers modifiers;
	unsafe_vector<idx_t> &offsets;
	string_t *result_data;
	bool flip_bytes;
};

static void ConstructSortKeyRecursive(SortKeyVectorData &vector_data, idx_t start, idx_t end, SortKeyConstructInfo &info);

template<class OP>
void TemplatedConstructSortKey(SortKeyVectorData &vector_data, idx_t start, idx_t end, SortKeyConstructInfo &info) {
	auto data = UnifiedVectorFormat::GetData<typename OP::TYPE>(vector_data.format);
	auto &offsets = info.offsets;
	for (idx_t r = start; r < end; r++) {
		auto result_index = vector_data.result_sel.get_index(r);
		auto &offset = offsets[result_index];
		auto result_ptr = data_ptr_cast(info.result_data[result_index].GetDataWriteable());
		auto idx = vector_data.format.sel->get_index(r);
		if (!vector_data.format.validity.RowIsValid(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
			break;
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

void ConstructSortKeyList(SortKeyVectorData &vector_data, idx_t start, idx_t end, SortKeyConstructInfo &info) {
	auto data = UnifiedVectorFormat::GetData<list_entry_t>(vector_data.format);
	auto &offsets = info.offsets;
	for (idx_t r = start; r < end; r++) {
		auto result_index = vector_data.result_sel.get_index(r);
		auto &offset = offsets[result_index];
		auto result_ptr = data_ptr_cast(info.result_data[result_index].GetDataWriteable());
		auto idx = vector_data.format.sel->get_index(r);
		if (!vector_data.format.validity.RowIsValid(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
			continue;
		}
		// valid value - write the validity byte
		result_ptr[offset++] = vector_data.valid_byte;

		auto list_entry = data[idx];
		// recurse and write the list elements
		if (list_entry.length > 0) {
			ConstructSortKeyRecursive(vector_data.child_data[0], list_entry.offset, list_entry.offset + list_entry.length, info);
		}

		// write the end-of-list delimiter
		result_ptr[offset++] = info.flip_bytes ? ~data_t(0) : 0;
	}
}

static void ConstructSortKeyRecursive(SortKeyVectorData &vector_data, idx_t start, idx_t end, SortKeyConstructInfo &info) {
	switch(vector_data.GetPhysicalType()) {
	case PhysicalType::INT32:
		TemplatedConstructSortKey<SortKeyConstantOperator<int32_t>>(vector_data, start, end, info);
		break;
	case PhysicalType::VARCHAR:
		if (vector_data.vec.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedConstructSortKey<SortKeyVarcharOperator>(vector_data, start, end, info);
		} else {
			throw NotImplementedException("FIXME: ConstructSortKey blob");
		}
		break;
	case PhysicalType::LIST:
		ConstructSortKeyList(vector_data, start, end, info);
		break;
	default:
		throw NotImplementedException("Unsupported type %s in ConstructSortKey", vector_data.vec.GetType());
	}
}

static void ConstructSortKey(SortKeyVectorData &vector_data, SortKeyConstructInfo &info) {
	ConstructSortKeyRecursive(vector_data, 0, vector_data.size, info);
}


static void CreateSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<CreateSortKeyBindData>();

	// prepare the sort key data
	vector<SortKeyVectorData> sort_key_data;
	for(idx_t c = 0; c < args.ColumnCount(); c += 2) {
		sort_key_data.emplace_back(args.data[c], args.size(), bind_data.modifiers[c / 2]);
	}

	// two phases
	// a) get the length of the final sorted key
	// b) allocate the sorted key and construct
	// we do all of this in a vectorized manner
	SortKeyLengthInfo key_lengths(args.size());
	for(auto &vector_data : sort_key_data) {
		GetSortKeyLength(vector_data, key_lengths);
	}
	// allocate the empty sort keys
	auto result_data = FlatVector::GetData<string_t>(result);
	for(idx_t r = 0; r < args.size(); r++) {
		result_data[r] = StringVector::EmptyString(result, key_lengths.variable_lengths[r] + key_lengths.constant_length);
	}

	unsafe_vector<idx_t> offsets;
	offsets.resize(args.size(), 0);
	// now construct the sort keys
	for(idx_t c = 0; c < sort_key_data.size(); c++) {
		SortKeyConstructInfo info(bind_data.modifiers[c], offsets, result_data);
		ConstructSortKey(sort_key_data[c], info);
	}
	// call Finalize on the result
	for(idx_t r = 0; r < args.size(); r++) {
		result_data[r].Finalize();
	}
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
