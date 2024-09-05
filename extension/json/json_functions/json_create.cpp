#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

using StructNames = unordered_map<string, unique_ptr<Vector>>;

struct JSONCreateFunctionData : public FunctionData {
public:
	explicit JSONCreateFunctionData(unordered_map<string, unique_ptr<Vector>> const_struct_names)
	    : const_struct_names(std::move(const_struct_names)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		// Have to do this because we can't implicitly copy Vector
		unordered_map<string, unique_ptr<Vector>> map_copy;
		for (const auto &kv : const_struct_names) {
			// The vectors are const vectors of the key value
			map_copy[kv.first] = make_uniq<Vector>(Value(kv.first));
		}
		return make_uniq<JSONCreateFunctionData>(std::move(map_copy));
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}

public:
	// Const struct name vectors live here so they don't have to be re-initialized for every DataChunk
	StructNames const_struct_names;
};

static LogicalType GetJSONType(StructNames &const_struct_names, const LogicalType &type) {
	if (type.IsJSONType()) {
		return type;
	}

	switch (type.id()) {
	// These types can go directly into JSON
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DOUBLE:
		return type;
	// We cast these types to a type that can go into JSON
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		return LogicalType::BIGINT;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return LogicalType::UBIGINT;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	// The nested types need to conform as well
	case LogicalTypeId::LIST:
		return LogicalType::LIST(GetJSONType(const_struct_names, ListType::GetChildType(type)));
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(GetJSONType(const_struct_names, ArrayType::GetChildType(type)),
		                          ArrayType::GetSize(type));
	// Struct and MAP are treated as JSON values
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> child_types;
		for (const auto &child_type : StructType::GetChildTypes(type)) {
			const_struct_names[child_type.first] = make_uniq<Vector>(Value(child_type.first));
			child_types.emplace_back(child_type.first, GetJSONType(const_struct_names, child_type.second));
		}
		return LogicalType::STRUCT(child_types);
	}
	case LogicalTypeId::MAP: {
		return LogicalType::MAP(LogicalType::VARCHAR, GetJSONType(const_struct_names, MapType::ValueType(type)));
	}
	case LogicalTypeId::UNION: {
		child_list_t<LogicalType> member_types;
		for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(type); member_idx++) {
			auto &member_name = UnionType::GetMemberName(type, member_idx);
			auto &member_type = UnionType::GetMemberType(type, member_idx);

			const_struct_names[member_name] = make_uniq<Vector>(Value(member_name));
			member_types.emplace_back(member_name, GetJSONType(const_struct_names, member_type));
		}
		return LogicalType::UNION(member_types);
	}
	// All other types (e.g. date) are cast to VARCHAR
	default:
		return LogicalTypeId::VARCHAR;
	}
}

static unique_ptr<FunctionData> JSONCreateBindParams(ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments, bool object) {
	unordered_map<string, unique_ptr<Vector>> const_struct_names;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &type = arguments[i]->return_type;
		if (arguments[i]->HasParameter()) {
			throw ParameterNotResolvedException();
		} else if (type == LogicalTypeId::SQLNULL) {
			// This is needed for macro's
			bound_function.arguments.push_back(type);
		} else if (object && i % 2 == 0) {
			// Key, must be varchar
			bound_function.arguments.push_back(LogicalType::VARCHAR);
		} else {
			// Value, cast to types that we can put in JSON
			bound_function.arguments.push_back(GetJSONType(const_struct_names, type));
		}
	}
	return make_uniq<JSONCreateFunctionData>(std::move(const_struct_names));
}

static unique_ptr<FunctionData> JSONObjectBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() % 2 != 0) {
		throw InvalidInputException("json_object() requires an even number of arguments");
	}
	return JSONCreateBindParams(bound_function, arguments, true);
}

static unique_ptr<FunctionData> JSONArrayBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	return JSONCreateBindParams(bound_function, arguments, false);
}

static unique_ptr<FunctionData> ToJSONBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw InvalidInputException("to_json() takes exactly one argument");
	}
	return JSONCreateBindParams(bound_function, arguments, false);
}

static unique_ptr<FunctionData> ArrayToJSONBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw InvalidInputException("array_to_json() takes exactly one argument");
	}
	auto arg_id = arguments[0]->return_type.id();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (arg_id != LogicalTypeId::LIST && arg_id != LogicalTypeId::SQLNULL) {
		throw InvalidInputException("array_to_json() argument type must be LIST");
	}
	return JSONCreateBindParams(bound_function, arguments, false);
}

static unique_ptr<FunctionData> RowToJSONBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw InvalidInputException("row_to_json() takes exactly one argument");
	}
	auto arg_id = arguments[0]->return_type.id();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (arguments[0]->return_type.id() != LogicalTypeId::STRUCT && arg_id != LogicalTypeId::SQLNULL) {
		throw InvalidInputException("row_to_json() argument type must be STRUCT");
	}
	return JSONCreateBindParams(bound_function, arguments, false);
}

template <class INPUT_TYPE, class RESULT_TYPE>
struct CreateJSONValue {
	static inline RESULT_TYPE Operation(const INPUT_TYPE &input) {
		throw NotImplementedException("Unsupported type for CreateJSONValue");
	}
};

template <class INPUT_TYPE>
struct CreateJSONValue<INPUT_TYPE, bool> {
	static inline yyjson_mut_val *Operation(yyjson_mut_doc *doc, const INPUT_TYPE &input) {
		return yyjson_mut_bool(doc, input);
	}
};

template <class INPUT_TYPE>
struct CreateJSONValue<INPUT_TYPE, uint64_t> {
	static inline yyjson_mut_val *Operation(yyjson_mut_doc *doc, const INPUT_TYPE &input) {
		return yyjson_mut_uint(doc, input);
	}
};

template <class INPUT_TYPE>
struct CreateJSONValue<INPUT_TYPE, int64_t> {
	static inline yyjson_mut_val *Operation(yyjson_mut_doc *doc, const INPUT_TYPE &input) {
		return yyjson_mut_sint(doc, input);
	}
};

template <class INPUT_TYPE>
struct CreateJSONValue<INPUT_TYPE, double> {
	static inline yyjson_mut_val *Operation(yyjson_mut_doc *doc, const INPUT_TYPE &input) {
		return yyjson_mut_real(doc, input);
	}
};

template <>
struct CreateJSONValue<string_t, string_t> {
	static inline yyjson_mut_val *Operation(yyjson_mut_doc *doc, const string_t &input) {
		return yyjson_mut_strncpy(doc, input.GetData(), input.GetSize());
	}
};

template <>
struct CreateJSONValue<hugeint_t, string_t> {
	static inline yyjson_mut_val *Operation(yyjson_mut_doc *doc, const hugeint_t &input) {
		const auto input_string = input.ToString();
		return yyjson_mut_strncpy(doc, input_string.c_str(), input_string.length());
	}
};

template <>
struct CreateJSONValue<uhugeint_t, string_t> {
	static inline yyjson_mut_val *Operation(yyjson_mut_doc *doc, const uhugeint_t &input) {
		const auto input_string = input.ToString();
		return yyjson_mut_strncpy(doc, input_string.c_str(), input_string.length());
	}
};

template <class T>
inline yyjson_mut_val *CreateJSONValueFromJSON(yyjson_mut_doc *doc, const T &value) {
	return nullptr; // This function should only be called with string_t as template
}

template <>
inline yyjson_mut_val *CreateJSONValueFromJSON(yyjson_mut_doc *doc, const string_t &value) {
	auto value_doc = JSONCommon::ReadDocument(value, JSONCommon::READ_FLAG, &doc->alc);
	auto result = yyjson_val_mut_copy(doc, value_doc->root);
	return result;
}

// Forward declaration so we can recurse for nested types
static void CreateValues(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v,
                         idx_t count);

static void AddKeyValuePairs(yyjson_mut_doc *doc, yyjson_mut_val *objs[], Vector &key_v, yyjson_mut_val *vals[],
                             idx_t count) {
	UnifiedVectorFormat key_data;
	key_v.ToUnifiedFormat(count, key_data);
	auto keys = UnifiedVectorFormat::GetData<string_t>(key_data);

	for (idx_t i = 0; i < count; i++) {
		auto key_idx = key_data.sel->get_index(i);
		if (!key_data.validity.RowIsValid(key_idx)) {
			continue;
		}
		auto key = CreateJSONValue<string_t, string_t>::Operation(doc, keys[key_idx]);
		yyjson_mut_obj_add(objs[i], key, vals[i]);
	}
}

static void CreateKeyValuePairs(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *objs[],
                                yyjson_mut_val *vals[], Vector &key_v, Vector &value_v, idx_t count) {
	CreateValues(names, doc, vals, value_v, count);
	AddKeyValuePairs(doc, objs, key_v, vals, count);
}

static void CreateValuesNull(yyjson_mut_doc *doc, yyjson_mut_val *vals[], idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		vals[i] = yyjson_mut_null(doc);
	}
}

template <class INPUT_TYPE, class TARGET_TYPE>
static void TemplatedCreateValues(yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v, idx_t count) {
	UnifiedVectorFormat value_data;
	value_v.ToUnifiedFormat(count, value_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);

	const auto type_is_json = value_v.GetType().IsJSONType();
	for (idx_t i = 0; i < count; i++) {
		idx_t val_idx = value_data.sel->get_index(i);
		if (!value_data.validity.RowIsValid(val_idx)) {
			vals[i] = yyjson_mut_null(doc);
		} else if (type_is_json) {
			vals[i] = CreateJSONValueFromJSON(doc, values[val_idx]);
		} else {
			vals[i] = CreateJSONValue<INPUT_TYPE, TARGET_TYPE>::Operation(doc, values[val_idx]);
		}
		D_ASSERT(vals[i] != nullptr);
	}
}

static void CreateValuesStruct(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v,
                               idx_t count) {
	// Structs become values, therefore we initialize vals to JSON values
	for (idx_t i = 0; i < count; i++) {
		vals[i] = yyjson_mut_obj(doc);
	}
	// Initialize re-usable array for the nested values
	auto nested_vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, count);

	// Add the key/value pairs to the values
	auto &entries = StructVector::GetEntries(value_v);
	for (idx_t entry_i = 0; entry_i < entries.size(); entry_i++) {
		auto &struct_key_v = *names.at(StructType::GetChildName(value_v.GetType(), entry_i));
		auto &struct_val_v = *entries[entry_i];
		CreateKeyValuePairs(names, doc, vals, nested_vals, struct_key_v, struct_val_v, count);
	}
	// Whole struct can be NULL
	UnifiedVectorFormat struct_data;
	value_v.ToUnifiedFormat(count, struct_data);
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = struct_data.sel->get_index(i);
		if (!struct_data.validity.RowIsValid(idx)) {
			vals[i] = yyjson_mut_null(doc);
		}
	}
}

static void CreateValuesMap(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v,
                            idx_t count) {
	// Create nested keys
	auto &map_key_v = MapVector::GetKeys(value_v);
	auto map_key_count = ListVector::GetListSize(value_v);
	Vector map_keys_string(LogicalType::VARCHAR, map_key_count);
	VectorOperations::DefaultCast(map_key_v, map_keys_string, map_key_count);
	auto nested_keys = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, map_key_count);
	TemplatedCreateValues<string_t, string_t>(doc, nested_keys, map_keys_string, map_key_count);
	// Create nested values
	auto &map_val_v = MapVector::GetValues(value_v);
	auto map_val_count = ListVector::GetListSize(value_v);
	auto nested_vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, map_val_count);
	CreateValues(names, doc, nested_vals, map_val_v, map_val_count);
	// Add the key/value pairs to the values
	UnifiedVectorFormat map_data;
	value_v.ToUnifiedFormat(count, map_data);
	auto map_key_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(map_data);
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = map_data.sel->get_index(i);
		if (!map_data.validity.RowIsValid(idx)) {
			// Whole map can be NULL
			vals[i] = yyjson_mut_null(doc);
		} else {
			vals[i] = yyjson_mut_obj(doc);
			const auto &key_list_entry = map_key_list_entries[idx];
			for (idx_t child_i = key_list_entry.offset; child_i < key_list_entry.offset + key_list_entry.length;
			     child_i++) {
				if (!unsafe_yyjson_is_null(nested_keys[child_i])) {
					yyjson_mut_obj_add(vals[i], nested_keys[child_i], nested_vals[child_i]);
				}
			}
		}
	}
}

static void CreateValuesUnion(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v,
                              idx_t count) {
	// Structs become values, therefore we initialize vals to JSON values
	UnifiedVectorFormat value_data;
	value_v.ToUnifiedFormat(count, value_data);
	if (value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			vals[i] = yyjson_mut_obj(doc);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto index = value_data.sel->get_index(i);
			if (!value_data.validity.RowIsValid(index)) {
				// Make the entry NULL if the Union value is NULL
				vals[i] = yyjson_mut_null(doc);
			} else {
				vals[i] = yyjson_mut_obj(doc);
			}
		}
	}

	// Initialize re-usable array for the nested values
	auto nested_vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, count);

	auto &tag_v = UnionVector::GetTags(value_v);
	UnifiedVectorFormat tag_data;
	tag_v.ToUnifiedFormat(count, tag_data);

	// Add the key/value pairs to the values
	for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(value_v.GetType()); member_idx++) {
		auto &member_val_v = UnionVector::GetMember(value_v, member_idx);
		auto &member_key_v = *names.at(UnionType::GetMemberName(value_v.GetType(), member_idx));

		// This implementation is not optimal since we convert the entire member vector,
		// and then skip the rows not matching the tag afterwards.

		CreateValues(names, doc, nested_vals, member_val_v, count);

		// This is a inlined copy of AddKeyValuePairs but we also skip null tags
		// and the rows where the member is not matching the tag
		UnifiedVectorFormat key_data;
		member_key_v.ToUnifiedFormat(count, key_data);
		auto keys = UnifiedVectorFormat::GetData<string_t>(key_data);

		for (idx_t i = 0; i < count; i++) {
			auto value_index = value_data.sel->get_index(i);
			if (!value_data.validity.RowIsValid(value_index)) {
				// This entry is just NULL in it's entirety
				continue;
			}
			auto tag_idx = tag_data.sel->get_index(i);
			if (!tag_data.validity.RowIsValid(tag_idx)) {
				continue;
			}
			auto tag = (UnifiedVectorFormat::GetData<uint8_t>(tag_data))[tag_idx];
			if (tag != member_idx) {
				continue;
			}
			auto key_idx = key_data.sel->get_index(i);
			if (!key_data.validity.RowIsValid(key_idx)) {
				continue;
			}
			auto key = CreateJSONValue<string_t, string_t>::Operation(doc, keys[key_idx]);
			yyjson_mut_obj_add(vals[i], key, nested_vals[i]);
		}
	}
}

static void CreateValuesList(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v,
                             idx_t count) {
	// Initialize array for the nested values
	auto &child_v = ListVector::GetEntry(value_v);
	auto child_count = ListVector::GetListSize(value_v);
	auto nested_vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, child_count);
	// Fill nested_vals with list values
	CreateValues(names, doc, nested_vals, child_v, child_count);
	// Now we add the values to the appropriate JSON arrays
	UnifiedVectorFormat list_data;
	value_v.ToUnifiedFormat(count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = list_data.sel->get_index(i);
		if (!list_data.validity.RowIsValid(idx)) {
			vals[i] = yyjson_mut_null(doc);
		} else {
			vals[i] = yyjson_mut_arr(doc);
			const auto &entry = list_entries[idx];
			for (idx_t child_i = entry.offset; child_i < entry.offset + entry.length; child_i++) {
				yyjson_mut_arr_append(vals[i], nested_vals[child_i]);
			}
		}
	}
}

static void CreateValuesArray(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v,
                              idx_t count) {

	value_v.Flatten(count);

	// Initialize array for the nested values
	auto &child_v = ArrayVector::GetEntry(value_v);
	auto array_size = ArrayType::GetSize(value_v.GetType());
	auto child_count = count * array_size;

	auto nested_vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, child_count);
	// Fill nested_vals with list values
	CreateValues(names, doc, nested_vals, child_v, child_count);
	// Now we add the values to the appropriate JSON arrays
	UnifiedVectorFormat list_data;
	value_v.ToUnifiedFormat(count, list_data);
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = list_data.sel->get_index(i);
		if (!list_data.validity.RowIsValid(idx)) {
			vals[i] = yyjson_mut_null(doc);
		} else {
			vals[i] = yyjson_mut_arr(doc);
			auto offset = idx * array_size;
			for (idx_t child_i = offset; child_i < offset + array_size; child_i++) {
				yyjson_mut_arr_append(vals[i], nested_vals[child_i]);
			}
		}
	}
}

static void CreateValues(const StructNames &names, yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v,
                         idx_t count) {
	switch (value_v.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		CreateValuesNull(doc, vals, count);
		break;
	case LogicalTypeId::BOOLEAN:
		TemplatedCreateValues<bool, bool>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::TINYINT:
		TemplatedCreateValues<int8_t, int64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::SMALLINT:
		TemplatedCreateValues<int16_t, int64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::INTEGER:
		TemplatedCreateValues<int32_t, int64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::BIGINT:
		TemplatedCreateValues<int64_t, int64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::HUGEINT:
		TemplatedCreateValues<hugeint_t, string_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::UHUGEINT:
		TemplatedCreateValues<uhugeint_t, string_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::UTINYINT:
		TemplatedCreateValues<uint8_t, uint64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::USMALLINT:
		TemplatedCreateValues<uint16_t, uint64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::UINTEGER:
		TemplatedCreateValues<uint32_t, uint64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedCreateValues<uint64_t, uint64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::FLOAT:
		TemplatedCreateValues<float, double>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::DOUBLE:
		TemplatedCreateValues<double, double>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::BIT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		TemplatedCreateValues<string_t, string_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::STRUCT:
		CreateValuesStruct(names, doc, vals, value_v, count);
		break;
	case LogicalTypeId::MAP:
		CreateValuesMap(names, doc, vals, value_v, count);
		break;
	case LogicalTypeId::LIST:
		CreateValuesList(names, doc, vals, value_v, count);
		break;
	case LogicalTypeId::UNION:
		CreateValuesUnion(names, doc, vals, value_v, count);
		break;
	case LogicalTypeId::ARRAY:
		CreateValuesArray(names, doc, vals, value_v, count);
		break;
	case LogicalTypeId::AGGREGATE_STATE:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::VARINT:
	case LogicalTypeId::UUID: {
		Vector string_vector(LogicalTypeId::VARCHAR, count);
		VectorOperations::DefaultCast(value_v, string_vector, count);
		TemplatedCreateValues<string_t, string_t>(doc, vals, string_vector, count);
		break;
	}
	case LogicalTypeId::DECIMAL: {
		Vector double_vector(LogicalType::DOUBLE, count);
		VectorOperations::DefaultCast(value_v, double_vector, count);
		TemplatedCreateValues<double, double>(doc, vals, double_vector, count);
		break;
	}
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
	case LogicalTypeId::ANY:
	case LogicalTypeId::USER:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::STRING_LITERAL:
	case LogicalTypeId::INTEGER_LITERAL:
	case LogicalTypeId::POINTER:
	case LogicalTypeId::VALIDITY:
	case LogicalTypeId::TABLE:
	case LogicalTypeId::LAMBDA:
		throw InternalException("Unsupported type arrived at JSON create function");
	}
}

static void ObjectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<JSONCreateFunctionData>();
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYAlc();

	// Initialize values
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument(alc);
	auto objs = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, count);
	for (idx_t i = 0; i < count; i++) {
		objs[i] = yyjson_mut_obj(doc);
	}
	// Initialize a re-usable value array
	auto vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, count);
	// Loop through key/value pairs
	for (idx_t pair_idx = 0; pair_idx < args.data.size() / 2; pair_idx++) {
		Vector &key_v = args.data[pair_idx * 2];
		Vector &value_v = args.data[pair_idx * 2 + 1];
		CreateKeyValuePairs(info.const_struct_names, doc, objs, vals, key_v, value_v, count);
	}
	// Write JSON values to string
	auto objects = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		objects[i] = JSONCommon::WriteVal<yyjson_mut_val>(objs[i], alc);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ArrayFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<JSONCreateFunctionData>();
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYAlc();

	// Initialize arrays
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument(alc);
	auto arrs = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, count);
	for (idx_t i = 0; i < count; i++) {
		arrs[i] = yyjson_mut_arr(doc);
	}
	// Initialize a re-usable value array
	auto vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, count);
	// Loop through args
	for (auto &v : args.data) {
		CreateValues(info.const_struct_names, doc, vals, v, count);
		for (idx_t i = 0; i < count; i++) {
			yyjson_mut_arr_append(arrs[i], vals[i]);
		}
	}
	// Write JSON arrays to string
	auto objects = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		objects[i] = JSONCommon::WriteVal<yyjson_mut_val>(arrs[i], alc);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ToJSONFunctionInternal(const StructNames &names, Vector &input, const idx_t count, Vector &result,
                                   yyjson_alc *alc) {
	// Initialize array for values
	auto doc = JSONCommon::CreateDocument(alc);
	auto vals = JSONCommon::AllocateArray<yyjson_mut_val *>(doc, count);
	CreateValues(names, doc, vals, input, count);

	// Write JSON values to string
	auto objects = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = input_data.sel->get_index(i);
		if (input_data.validity.RowIsValid(idx)) {
			objects[i] = JSONCommon::WriteVal<yyjson_mut_val>(vals[i], alc);
		} else {
			result_validity.SetInvalid(i);
		}
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR || count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ToJSONFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<JSONCreateFunctionData>();
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYAlc();

	ToJSONFunctionInternal(info.const_struct_names, args.data[0], args.size(), result, alc);
}

ScalarFunctionSet JSONFunctions::GetObjectFunction() {
	ScalarFunction fun("json_object", {}, LogicalType::JSON(), ObjectFunction, JSONObjectBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetArrayFunction() {
	ScalarFunction fun("json_array", {}, LogicalType::JSON(), ArrayFunction, JSONArrayBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetToJSONFunction() {
	ScalarFunction fun("to_json", {}, LogicalType::JSON(), ToJSONFunction, ToJSONBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetArrayToJSONFunction() {
	ScalarFunction fun("array_to_json", {}, LogicalType::JSON(), ToJSONFunction, ArrayToJSONBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetRowToJSONFunction() {
	ScalarFunction fun("row_to_json", {}, LogicalType::JSON(), ToJSONFunction, RowToJSONBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	return ScalarFunctionSet(fun);
}

struct NestedToJSONCastData : public BoundCastData {
public:
	NestedToJSONCastData() {
	}

	unique_ptr<BoundCastData> Copy() const override {
		auto result = make_uniq<NestedToJSONCastData>();
		for (auto &csn : const_struct_names) {
			result->const_struct_names.emplace(csn.first, make_uniq<Vector>(csn.second->GetValue(0)));
		}
		return std::move(result);
	}

public:
	StructNames const_struct_names;
};

static bool AnyToJSONCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = parameters.local_state->Cast<JSONFunctionLocalState>();
	lstate.json_allocator.Reset();
	auto alc = lstate.json_allocator.GetYYAlc();
	const auto &names = parameters.cast_data->Cast<NestedToJSONCastData>().const_struct_names;

	ToJSONFunctionInternal(names, source, count, result, alc);
	return true;
}

BoundCastInfo AnyToJSONCastBind(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	auto cast_data = make_uniq<NestedToJSONCastData>();
	GetJSONType(cast_data->const_struct_names, source);
	return BoundCastInfo(AnyToJSONCast, std::move(cast_data), JSONFunctionLocalState::InitCastLocalState);
}

void JSONFunctions::RegisterJSONCreateCastFunctions(CastFunctionSet &casts) {
	// Anything can be cast to JSON
	for (const auto &type : LogicalType::AllTypes()) {
		LogicalType source_type;
		switch (type.id()) {
		case LogicalTypeId::STRUCT:
			source_type = LogicalType::STRUCT({{"any", LogicalType::ANY}});
			break;
		case LogicalTypeId::LIST:
			source_type = LogicalType::LIST(LogicalType::ANY);
			break;
		case LogicalTypeId::MAP:
			source_type = LogicalType::MAP(LogicalType::ANY, LogicalType::ANY);
			break;
		case LogicalTypeId::UNION:
			source_type = LogicalType::UNION({{"any", LogicalType::ANY}});
			break;
		case LogicalTypeId::ARRAY:
			source_type = LogicalType::ARRAY(LogicalType::ANY, optional_idx());
			break;
		case LogicalTypeId::VARCHAR:
			// We skip this one here as it's handled in json_functions.cpp
			continue;
		default:
			source_type = type;
		}
		// We prefer going to JSON over going to VARCHAR if a function can do either
		const auto source_to_json_cost =
		    MaxValue<int64_t>(casts.ImplicitCastCost(source_type, LogicalType::VARCHAR) - 1, 0);
		casts.RegisterCastFunction(source_type, LogicalType::JSON(), AnyToJSONCastBind, source_to_json_cost);
	}
}

} // namespace duckdb
