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

static void GetSortKeyLength(Vector &vec, UnifiedVectorFormat &udata, idx_t size, idx_t &constant_length, unsafe_vector<idx_t> &variable_lengths) {
	auto physical_type = vec.GetType().InternalType();
	// every row is prefixed by a validity byte
	constant_length += 1;
	if (TypeIsConstantSize(physical_type)) {
		constant_length += GetTypeIdSize(physical_type);
		return;
	}
	// handle variable lengths
	switch(physical_type) {
	default:
		throw InternalException("Unsupported physical type in GetSortKeyLength", physical_type);
	}
}

template<class T>
void ConstructSortKeyConstant(UnifiedVectorFormat &udata, idx_t size, OrderModifiers modifiers, unsafe_vector<idx_t> &offsets, string_t *result_data) {
	auto data = UnifiedVectorFormat::GetData<T>(udata);
	data_t null_byte = modifiers.null_type == OrderByNullType::NULLS_LAST ? 1 : 0;
	data_t valid_byte = 1 - null_byte;
	bool flip_bytes = modifiers.order_type == OrderType::DESCENDING;
	for(idx_t r = 0; r < size; r++) {
		auto ptr = data_ptr_cast(result_data[r].GetDataWriteable());
		auto idx = udata.sel->get_index(r);
		if (!udata.validity.RowIsValid(idx)) {
			// NULL value - write 1 as the validity byte and skip
			ptr[offsets[r]] = null_byte;
			offsets[r]++;
			break;
		}
		// valid value - write 0 as the validity byte
		ptr[offsets[r]] = valid_byte;
		offsets[r]++;
		Radix::EncodeData<T>(ptr + offsets[r], data[idx]);
		if (flip_bytes) {
			// descending order - so flip bytes
			for(idx_t b = offsets[r]; b < offsets[r] + sizeof(T); b++) {
				ptr[b] = ~ptr[b];
			}
		}
		offsets[r] += sizeof(T) + 1;
	}
}

static void ConstructSortKey(Vector &vec, UnifiedVectorFormat &udata, idx_t size, OrderModifiers modifiers, unsafe_vector<idx_t> &offsets, string_t *result_data) {
	switch(vec.GetType().InternalType()) {
	case PhysicalType::INT32:
		ConstructSortKeyConstant<int32_t>(udata, size, modifiers, offsets, result_data);
		break;
	default:
		throw InternalException("Unsupported physical type in ConstructSortKey");
	}
}

static void CreateSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<CreateSortKeyBindData>();
	idx_t constant_length = 0;

	unsafe_vector<idx_t> variable_lengths;
	variable_lengths.resize(args.size(), 0);

	auto unified_formats = args.ToUnifiedFormat();

	// two phases
	// a) get the length of the final sorted key
	// b) allocate the sorted key and construct
	// we do all of this in a vectorized manner
	for(idx_t c = 0; c < args.ColumnCount(); c += 2) {
		GetSortKeyLength(args.data[c], unified_formats[c], args.size(), constant_length, variable_lengths);
	}
	// allocate the empty sort keys
	auto result_data = FlatVector::GetData<string_t>(result);
	for(idx_t r = 0; r < args.size(); r++) {
		result_data[r] = StringVector::EmptyString(result, variable_lengths[r] + constant_length);
	}

	unsafe_vector<idx_t> offsets;
	offsets.resize(args.size(), 0);
	// now construct the sort keys
	for(idx_t c = 0; c < args.ColumnCount(); c += 2) {
		ConstructSortKey(args.data[c], unified_formats[c], args.size(), bind_data.modifiers[c / 2], offsets, result_data);
	}
	for(idx_t r = 0; r < args.size(); r++) {
		result_data[r].Finalize();
	}
}

ScalarFunction CreateSortKeyFun::GetFunction() {
	ScalarFunction sort_key_function({LogicalType::ANY}, LogicalType::BLOB, CreateSortKeyFunction, CreateSortKeyBind);
	sort_key_function.varargs = LogicalType::ANY;
	sort_key_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return sort_key_function;
}

} // namespace duckdb
