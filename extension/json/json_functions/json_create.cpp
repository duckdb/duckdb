#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

struct JSONCreateFunctionData : public FunctionData {
public:
	explicit JSONCreateFunctionData(unordered_map<string, duckdb::unique_ptr<Vector>> const_struct_names)
	    : const_struct_names(std::move(const_struct_names)) {
	}
	duckdb::unique_ptr<FunctionData> Copy() const override {
		// Have to do this because we can't implicitly copy Vector
		unordered_map<string, duckdb::unique_ptr<Vector>> map_copy;
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
	unordered_map<string, duckdb::unique_ptr<Vector>> const_struct_names;
};

static LogicalType GetJSONType(unordered_map<string, duckdb::unique_ptr<Vector>> &const_struct_names,
                               const LogicalType &type) {
	if (JSONCommon::LogicalTypeIsJSON(type)) {
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
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	// The nested types need to conform as well
	case LogicalTypeId::LIST:
		return LogicalType::LIST(GetJSONType(const_struct_names, ListType::GetChildType(type)));
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

static duckdb::unique_ptr<FunctionData>
JSONCreateBindParams(ScalarFunction &bound_function, vector<duckdb::unique_ptr<Expression>> &arguments, bool object) {
	unordered_map<string, duckdb::unique_ptr<Vector>> const_struct_names;
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

static duckdb::unique_ptr<FunctionData> JSONObjectBind(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<duckdb::unique_ptr<Expression>> &arguments) {
	if (arguments.size() % 2 != 0) {
		throw InvalidInputException("json_object() requires an even number of arguments");
	}
	return JSONCreateBindParams(bound_function, arguments, true);
}

static duckdb::unique_ptr<FunctionData> JSONArrayBind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<duckdb::unique_ptr<Expression>> &arguments) {
	return JSONCreateBindParams(bound_function, arguments, false);
}

static duckdb::unique_ptr<FunctionData> ToJSONBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<duckdb::unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw InvalidInputException("to_json() takes exactly one argument");
	}
	return JSONCreateBindParams(bound_function, arguments, false);
}

static duckdb::unique_ptr<FunctionData> ArrayToJSONBind(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<duckdb::unique_ptr<Expression>> &arguments) {
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

static duckdb::unique_ptr<FunctionData> RowToJSONBind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<duckdb::unique_ptr<Expression>> &arguments) {
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
	return yyjson_mut_strn(doc, value.GetData(), value.GetSize());
}

inline yyjson_mut_val *CreateJSONValueFromJSON(yyjson_mut_doc *doc, const string_t &value) {
	auto value_doc = JSONCommon::ReadDocument(value, JSONCommon::READ_FLAG, &doc->alc);
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

	const auto type_is_json = JSONCommon::LogicalTypeIsJSON(value_v.GetType());
	for (idx_t i = 0; i < count; i++) {
		idx_t val_idx = value_data.sel->get_index(i);
		if (!value_data.validity.RowIsValid(val_idx)) {
			vals[i] = yyjson_mut_null(doc);
		} else if (type_is_json) {
			vals[i] = CreateJSONValueFromJSON(doc, (string_t &)values[val_idx]);
		} else {
			vals[i] = CreateJSONValue<T>(doc, values[val_idx]);
		}
		D_ASSERT(vals[i] != nullptr);
	}
}

static void CreateValuesStruct(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                               Vector &value_v, idx_t count) {
	// Structs become values, therefore we initialize vals to JSON values
	for (idx_t i = 0; i < count; i++) {
		vals[i] = yyjson_mut_obj(doc);
	}
	// Initialize re-usable array for the nested values
	auto nested_vals = (yyjson_mut_val **)doc->alc.malloc(doc->alc.ctx, sizeof(yyjson_mut_val *) * count);

	// Add the key/value pairs to the values
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
	// Create nested keys
	auto &map_key_v = MapVector::GetKeys(value_v);
	auto map_key_count = ListVector::GetListSize(value_v);
	auto nested_keys = (yyjson_mut_val **)doc->alc.malloc(doc->alc.ctx, sizeof(yyjson_mut_val *) * map_key_count);
	TemplatedCreateValues<string_t>(doc, nested_keys, map_key_v, map_key_count);
	// Create nested values
	auto &map_val_v = MapVector::GetValues(value_v);
	auto map_val_count = ListVector::GetListSize(value_v);
	auto nested_vals = (yyjson_mut_val **)doc->alc.malloc(doc->alc.ctx, sizeof(yyjson_mut_val *) * map_val_count);
	CreateValues(info, doc, nested_vals, map_val_v, map_val_count);
	// Add the key/value pairs to the values
	UnifiedVectorFormat map_data;
	value_v.ToUnifiedFormat(count, map_data);
	auto map_key_list_entries = (list_entry_t *)map_data.data;
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

static void CreateValuesUnion(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                              Vector &value_v, idx_t count) {
	// Structs become values, therefore we initialize vals to JSON values
	for (idx_t i = 0; i < count; i++) {
		vals[i] = yyjson_mut_obj(doc);
	}

	// Initialize re-usable array for the nested values
	auto nested_vals = (yyjson_mut_val **)doc->alc.malloc(doc->alc.ctx, sizeof(yyjson_mut_val *) * count);

	auto &tag_v = UnionVector::GetTags(value_v);
	UnifiedVectorFormat tag_data;
	tag_v.ToUnifiedFormat(count, tag_data);

	// Add the key/value pairs to the values
	for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(value_v.GetType()); member_idx++) {
		auto &member_val_v = UnionVector::GetMember(value_v, member_idx);
		auto &member_key_v = *info.const_struct_names.at(UnionType::GetMemberName(value_v.GetType(), member_idx));

		// This implementation is not optimal since we convert the entire member vector,
		// and then skip the rows not matching the tag afterwards.

		CreateValues(info, doc, nested_vals, member_val_v, count);

		// This is a inlined copy of AddKeyValuePairs but we also skip null tags
		// and the rows where the member is not matching the tag
		UnifiedVectorFormat key_data;
		member_key_v.ToUnifiedFormat(count, key_data);
		auto keys = (string_t *)key_data.data;

		for (idx_t i = 0; i < count; i++) {
			auto tag_idx = tag_data.sel->get_index(i);
			if (!tag_data.validity.RowIsValid(tag_idx)) {
				continue;
			}
			auto tag = ((uint8_t *)tag_data.data)[tag_idx];
			if (tag != member_idx) {
				continue;
			}
			auto key_idx = key_data.sel->get_index(i);
			if (!key_data.validity.RowIsValid(key_idx)) {
				continue;
			}
			auto key = CreateJSONValue<string_t>(doc, keys[key_idx]);
			yyjson_mut_obj_add(vals[i], key, nested_vals[i]);
		}
	}
}

static void CreateValuesList(const JSONCreateFunctionData &info, yyjson_mut_doc *doc, yyjson_mut_val *vals[],
                             Vector &value_v, idx_t count) {
	// Initialize array for the nested values
	auto &child_v = ListVector::GetEntry(value_v);
	auto child_count = ListVector::GetListSize(value_v);
	auto nested_vals = (yyjson_mut_val **)doc->alc.malloc(doc->alc.ctx, sizeof(yyjson_mut_val *) * child_count);
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
	case LogicalTypeId::UNION:
		CreateValuesUnion(info, doc, vals, value_v, count);
		break;
	default:
		throw InternalException("Unsupported type arrived at JSON create function");
	}
}

static void ObjectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = (JSONCreateFunctionData &)*func_expr.bind_info;
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYJSONAllocator();

	// Initialize values
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument(alc);
	yyjson_mut_val *objs[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		objs[i] = yyjson_mut_obj(doc);
	}
	// Initialize a re-usable value array
	yyjson_mut_val *vals[STANDARD_VECTOR_SIZE];
	// Loop through key/value pairs
	for (idx_t pair_idx = 0; pair_idx < args.data.size() / 2; pair_idx++) {
		Vector &key_v = args.data[pair_idx * 2];
		Vector &value_v = args.data[pair_idx * 2 + 1];
		CreateKeyValuePairs(info, doc, objs, vals, key_v, value_v, count);
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
	const auto &info = (JSONCreateFunctionData &)*func_expr.bind_info;
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYJSONAllocator();

	// Initialize arrays
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument(alc);
	yyjson_mut_val *arrs[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		arrs[i] = yyjson_mut_arr(doc);
	}
	// Initialize a re-usable value array
	yyjson_mut_val *vals[STANDARD_VECTOR_SIZE];
	// Loop through args
	for (auto &v : args.data) {
		CreateValues(info, doc, vals, v, count);
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

static void ToJSONFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = (JSONCreateFunctionData &)*func_expr.bind_info;
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYJSONAllocator();

	// Initialize array for values
	const idx_t count = args.size();
	auto doc = JSONCommon::CreateDocument(alc);
	yyjson_mut_val *vals[STANDARD_VECTOR_SIZE];
	CreateValues(info, doc, vals, args.data[0], count);
	// Write JSON values to string
	auto objects = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(count, input_data);
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = input_data.sel->get_index(i);
		if (input_data.validity.RowIsValid(idx)) {
			objects[i] = JSONCommon::WriteVal<yyjson_mut_val>(vals[i], alc);
		} else {
			result_validity.SetInvalid(i);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet JSONFunctions::GetObjectFunction() {
	ScalarFunction fun("json_object", {}, JSONCommon::JSONType(), ObjectFunction, JSONObjectBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetArrayFunction() {
	ScalarFunction fun("json_array", {}, JSONCommon::JSONType(), ArrayFunction, JSONArrayBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetToJSONFunction() {
	ScalarFunction fun("to_json", {}, JSONCommon::JSONType(), ToJSONFunction, ToJSONBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetArrayToJSONFunction() {
	ScalarFunction fun("array_to_json", {}, JSONCommon::JSONType(), ToJSONFunction, ArrayToJSONBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetRowToJSONFunction() {
	ScalarFunction fun("row_to_json", {}, JSONCommon::JSONType(), ToJSONFunction, RowToJSONBind, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::ANY;
	return ScalarFunctionSet(fun);
}

} // namespace duckdb
