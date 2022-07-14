#include "json_common.hpp"
#include "json_functions.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

struct JSONCreateFunctionData : public FunctionData {
public:
	JSONCreateFunctionData(unordered_map<string, unique_ptr<Vector>> const_struct_names)
	    : const_struct_names(move(const_struct_names)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		// Have to do this because we can't implicitly copy Vector
		unordered_map<string, unique_ptr<Vector>> map_copy;
		for (const auto &kv : const_struct_names) {
			// The vectors are const vectors of the key value
			map_copy[kv.first] = make_unique<Vector>(Value(kv.first));
		}
		return make_unique<JSONCreateFunctionData>(move(map_copy));
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}

public:
	// Const struct name vectors live here so they don't have to be re-initialized for every DataChunk
	unordered_map<string, unique_ptr<Vector>> const_struct_names;
};

static LogicalType GetJSONType(unordered_map<string, unique_ptr<Vector>> &const_struct_names, const LogicalType &type) {
	switch (type.id()) {
	// These types can go directly into JSON
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::JSON:
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
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	// The nested types need to conform as well
	case LogicalTypeId::LIST:
		return LogicalType::LIST(GetJSONType(const_struct_names, ListType::GetChildType(type)));
	// Struct and MAP are treated as JSON objects
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> child_types;
		for (const auto &child_type : StructType::GetChildTypes(type)) {
			const_struct_names[child_type.first] = make_unique<Vector>(Value(child_type.first));
			child_types.emplace_back(child_type.first, GetJSONType(const_struct_names, child_type.second));
		}
		return LogicalType::STRUCT(child_types);
	}
	case LogicalTypeId::MAP: {
		return LogicalType::MAP(LogicalType::VARCHAR, GetJSONType(const_struct_names, MapType::ValueType(type)));
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
	return make_unique<JSONCreateFunctionData>(move(const_struct_names));
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

template <class T>
static inline yyjson_mut_val *CreateJSONValue(yyjson_mut_doc *doc, const T &value) {
	throw NotImplementedException("Unsupported type for CreateJSONValue");
}

template <>
inline yyjson_mut_val *CreateJSONValue(yyjson_mut_doc *doc, const bool &value) {
	return yyjson_mut_bool(doc, value);
}

template <>
inline yyjson_mut_val *CreateJSONValue(yyjson_mut_doc *doc, const uint64_t &value) {
	return yyjson_mut_uint(doc, value);
}

template <>
inline yyjson_mut_val *CreateJSONValue(yyjson_mut_doc *doc, const int64_t &value) {
	return yyjson_mut_sint(doc, value);
}

template <>
inline yyjson_mut_val *CreateJSONValue(yyjson_mut_doc *doc, const double &value) {
	return yyjson_mut_real(doc, value);
}

template <>
inline yyjson_mut_val *CreateJSONValue(yyjson_mut_doc *doc, const string_t &value) {
	return yyjson_mut_strn(doc, value.GetDataUnsafe(), value.GetSize());
}

inline yyjson_mut_val *CreateJSONValueFromJSON(yyjson_mut_doc *doc, const string_t &value) {
	auto value_doc = JSONCommon::ReadDocument(value);
	auto result = yyjson_val_mut_copy(doc, value_doc->root);
	return result;
}

// Forward declaration so we can recurse for nested types
static void CreateValues(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                         Vector &value_v, idx_t count);

static void AddKeyValuePairs(yyjson_mut_doc *doc, yyjson_mut_val *objs[], Vector &key_v, yyjson_mut_val *vals[],
                             idx_t count) {
	UnifiedVectorFormat key_data;
	key_v.ToUnifiedFormat(count, key_data);
	auto keys = (string_t *)key_data.data;

	for (idx_t i = 0; i < count; i++) {
		auto key_idx = key_data.sel->get_index(i);
		if (!key_data.validity.RowIsValid(key_idx)) {
			continue;
		}
		auto key = CreateJSONValue<string_t>(doc, keys[key_idx]);
		yyjson_mut_obj_add(objs[i], key, vals[i]);
	}
}

static void CreateKeyValuePairs(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *objs[],
                                yyjson_mut_val *vals[], Vector &key_v, Vector &value_v, idx_t count) {
	CreateValues(info, doc, vals, value_v, count);
	AddKeyValuePairs(doc, objs, key_v, vals, count);
}

static void CreateValuesNull(yyjson_mut_doc *doc, yyjson_mut_val *vals[], idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		vals[i] = yyjson_mut_null(doc);
	}
}

template <class T>
static void TemplatedCreateValues(yyjson_mut_doc *doc, yyjson_mut_val *vals[], Vector &value_v, idx_t count) {
	UnifiedVectorFormat value_data;
	value_v.ToUnifiedFormat(count, value_data);
	auto values = (T *)value_data.data;

	const auto value_type = value_v.GetType().id();
	for (idx_t i = 0; i < count; i++) {
		idx_t val_idx = value_data.sel->get_index(i);
		if (!value_data.validity.RowIsValid(val_idx)) {
			vals[i] = yyjson_mut_null(doc);
		} else if (value_type == LogicalTypeId::JSON) {
			vals[i] = CreateJSONValueFromJSON(doc, (string_t &)values[val_idx]);
		} else {
			vals[i] = CreateJSONValue<T>(doc, values[val_idx]);
		}
		D_ASSERT(vals[i] != nullptr);
	}
}

static void CreateValuesStruct(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                               Vector &value_v, idx_t count) {
	// Structs become objects, therefore we initialize vals to JSON objects
	for (idx_t i = 0; i < count; i++) {
		vals[i] = yyjson_mut_obj(doc);
	}
	// Initialize re-usable array for the nested values
	auto nested_vals_ptr = unique_ptr<yyjson_mut_val *[]>(new yyjson_mut_val *[count]);
	auto nested_vals = nested_vals_ptr.get();
	// Add the key/value pairs to the objects
	auto &entries = StructVector::GetEntries(value_v);
	for (idx_t entry_i = 0; entry_i < entries.size(); entry_i++) {
		auto &struct_key_v = *info.const_struct_names.at(StructType::GetChildName(value_v.GetType(), entry_i));
		auto &struct_val_v = *entries[entry_i];
		CreateKeyValuePairs(info, doc, vals, nested_vals, struct_key_v, struct_val_v, count);
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

static void CreateValuesMap(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                            Vector &value_v, idx_t count) {
	auto &entries = StructVector::GetEntries(value_v);
	// Create nested keys
	auto &map_key_list_v = *entries[0];
	auto &map_key_v = ListVector::GetEntry(map_key_list_v);
	auto map_key_count = ListVector::GetListSize(map_key_list_v);
	auto nested_keys_ptr = unique_ptr<yyjson_mut_val *[]>(new yyjson_mut_val *[map_key_count]);
	auto nested_keys = nested_keys_ptr.get();
	TemplatedCreateValues<string_t>(doc, nested_keys, map_key_v, map_key_count);
	// Create nested values
	auto &map_val_list_v = *entries[1];
	auto &map_val_v = ListVector::GetEntry(map_val_list_v);
	auto map_val_count = ListVector::GetListSize(map_val_list_v);
	auto nested_vals_ptr = unique_ptr<yyjson_mut_val *[]>(new yyjson_mut_val *[map_val_count]);
	auto nested_vals = nested_vals_ptr.get();
	CreateValues(info, doc, nested_vals, map_val_v, map_val_count);
	// Add the key/value pairs to the objects
	UnifiedVectorFormat map_data;
	value_v.ToUnifiedFormat(count, map_data);
	UnifiedVectorFormat map_key_list_data;
	map_key_list_v.ToUnifiedFormat(map_key_count, map_key_list_data);
	auto map_key_list_entries = (list_entry_t *)map_key_list_data.data;
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = map_data.sel->get_index(i);
		if (!map_data.validity.RowIsValid(idx)) {
			// Whole map can be NULL
			vals[i] = yyjson_mut_null(doc);
		} else {
			vals[i] = yyjson_mut_obj(doc);
			idx_t key_idx = map_key_list_data.sel->get_index(i);
			const auto &key_list_entry = map_key_list_entries[key_idx];
			for (idx_t child_i = key_list_entry.offset; child_i < key_list_entry.offset + key_list_entry.length;
			     child_i++) {
				if (!unsafe_yyjson_is_null(nested_keys[child_i])) {
					yyjson_mut_obj_add(vals[i], nested_keys[child_i], nested_vals[child_i]);
				}
			}
		}
	}
}

static void CreateValuesList(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                             Vector &value_v, idx_t count) {
	// Initialize array for the nested values
	auto &child_v = ListVector::GetEntry(value_v);
	auto child_count = ListVector::GetListSize(value_v);
	auto nested_vals_ptr = unique_ptr<yyjson_mut_val *[]>(new yyjson_mut_val *[child_count]);
	auto nested_vals = nested_vals_ptr.get();
	// Fill nested_vals with list values
	CreateValues(info, doc, nested_vals, child_v, child_count);
	// Now we add the values to the appropriate JSON arrays
	UnifiedVectorFormat list_data;
	value_v.ToUnifiedFormat(count, list_data);
	auto list_entries = (list_entry_t *)list_data.data;
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

static void CreateValues(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                         Vector &value_v, idx_t count) {
	switch (value_v.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		CreateValuesNull(doc, vals, count);
		break;
	case LogicalTypeId::BOOLEAN:
		TemplatedCreateValues<bool>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::BIGINT:
		TemplatedCreateValues<int64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedCreateValues<uint64_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::DOUBLE:
		TemplatedCreateValues<double>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		TemplatedCreateValues<string_t>(doc, vals, value_v, count);
		break;
	case LogicalTypeId::STRUCT:
		CreateValuesStruct(info, doc, vals, value_v, count);
		break;
	case LogicalTypeId::MAP:
		CreateValuesMap(info, doc, vals, value_v, count);
		break;
	case LogicalTypeId::LIST:
		CreateValuesList(info, doc, vals, value_v, count);
		break;
	default:
		throw InternalException("Unsupported type arrived at JSON create function");
	}
}

static void ObjectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONCreateFunctionData &)*func_expr.bind_info;
	// Initialize objects
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument();
	yyjson_mut_val *objs[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		objs[i] = yyjson_mut_obj(*doc);
	}
	// Initialize a re-usable value array
	yyjson_mut_val *vals[STANDARD_VECTOR_SIZE];
	// Loop through key/value pairs
	for (idx_t pair_idx = 0; pair_idx < args.data.size() / 2; pair_idx++) {
		Vector &key_v = args.data[pair_idx * 2];
		Vector &value_v = args.data[pair_idx * 2 + 1];
		CreateKeyValuePairs(info, *doc, objs, vals, key_v, value_v, count);
	}
	// Write JSON objects to string
	auto objects = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		yyjson_mut_doc_set_root(*doc, objs[i]);
		objects[i] = JSONCommon::WriteDoc(*doc, result);
	}
}

static void ArrayFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONCreateFunctionData &)*func_expr.bind_info;
	// Initialize arrays
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument();
	yyjson_mut_val *arrs[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		arrs[i] = yyjson_mut_arr(*doc);
	}
	// Initialize a re-usable value array
	yyjson_mut_val *vals[STANDARD_VECTOR_SIZE];
	// Loop through args
	for (auto &v : args.data) {
		CreateValues(info, *doc, vals, v, count);
		for (idx_t i = 0; i < count; i++) {
			yyjson_mut_arr_append(arrs[i], vals[i]);
		}
	}
	// Write JSON arrays to string
	auto objects = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		yyjson_mut_doc_set_root(*doc, arrs[i]);
		objects[i] = JSONCommon::WriteDoc(*doc, result);
	}
}

static void ToJSONFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONCreateFunctionData &)*func_expr.bind_info;
	// Initialize array for values
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument();
	yyjson_mut_val *vals[STANDARD_VECTOR_SIZE];
	CreateValues(info, *doc, vals, args.data[0], count);
	// Write JSON values to string
	auto objects = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(count, input_data);
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = input_data.sel->get_index(i);
		if (input_data.validity.RowIsValid(idx)) {
			yyjson_mut_doc_set_root(*doc, vals[i]);
			objects[i] = JSONCommon::WriteDoc(*doc, result);
		} else {
			result_validity.SetInvalid(i);
		}
	}
}

CreateScalarFunctionInfo JSONFunctions::GetObjectFunction() {
	auto fun = ScalarFunction("json_object", {}, LogicalType::JSON, ObjectFunction, JSONObjectBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return CreateScalarFunctionInfo(fun);
}

CreateScalarFunctionInfo JSONFunctions::GetArrayFunction() {
	auto fun = ScalarFunction("json_array", {}, LogicalType::JSON, ArrayFunction, JSONArrayBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return CreateScalarFunctionInfo(fun);
}

CreateScalarFunctionInfo JSONFunctions::GetToJSONFunction() {
	auto fun = ScalarFunction("to_json", {}, LogicalType::JSON, ToJSONFunction, ToJSONBind);
	fun.varargs = LogicalType::ANY;
	return CreateScalarFunctionInfo(fun);
}

CreateScalarFunctionInfo JSONFunctions::GetArrayToJSONFunction() {
	auto fun = ScalarFunction("array_to_json", {}, LogicalType::JSON, ToJSONFunction, ArrayToJSONBind);
	fun.varargs = LogicalType::ANY;
	return CreateScalarFunctionInfo(fun);
}

CreateScalarFunctionInfo JSONFunctions::GetRowToJSONFunction() {
	auto fun = ScalarFunction("row_to_json", {}, LogicalType::JSON, ToJSONFunction, RowToJSONBind);
	fun.varargs = LogicalType::ANY;
	return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
