#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

struct JSONObjectFunctionData : public FunctionData {
public:
	JSONObjectFunctionData(unordered_map<string, unique_ptr<Vector>> const_struct_names)
	    : const_struct_names(move(const_struct_names)) {
	}
	unique_ptr<FunctionData> Copy() override {
		// Have to do this because we can't implicitly copy Vector
		unordered_map<string, unique_ptr<Vector>> map_copy;
		for (const auto &kv : const_struct_names) {
			// The vectors are const vectors of the key value
			map_copy[kv.first] = make_unique<Vector>(Value(kv.first));
		}
		return make_unique<JSONObjectFunctionData>(move(map_copy));
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

static unique_ptr<FunctionData> JSONObjectBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() % 2 != 0) {
		throw InvalidInputException("json_object() requires an even number of arguments");
	}
	unordered_map<string, unique_ptr<Vector>> const_struct_names;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &type = arguments[i]->return_type;
		if (type == LogicalTypeId::SQLNULL) {
			// This is needed for macro's
			bound_function.arguments.push_back(type);
		} else if (i % 2 == 0) {
			// Key, must be varchar
			bound_function.arguments.push_back(LogicalType::VARCHAR);
		} else {
			// Value, cast to types that we can put in JSON
			bound_function.arguments.push_back(GetJSONType(const_struct_names, type));
		}
	}
	return make_unique<JSONObjectFunctionData>(move(const_struct_names));
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
	yyjson_doc_free(value_doc);
	return result;
}

template <class T>
static void TemplatedAddKeyValue(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                                 Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count,
                                 bool in_list) {
	VectorData key_data;
	key_v.Orrify(vcount, key_data);
	auto keys = (string_t *)key_data.data;

	VectorData value_data;
	value_v.Orrify(vcount, value_data);
	auto values = (T *)value_data.data;

	const auto value_type = value_v.GetType().id();
	for (idx_t i = 0; i < count; i++) {
		// Create key
		idx_t key_idx = key_data.sel->get_index(i) + offset;
		if (!key_data.validity.RowIsValid(key_idx)) {
			// Key is NULL, we can't add
			continue;
		}
		auto key = CreateJSONValue<string_t>(docs[i], keys[key_idx]);
		// Create value
		idx_t val_idx = value_data.sel->get_index(i) + offset;
		yyjson_mut_val *val;
		auto doc = in_list ? docs[0] : docs[i];
		if (!value_data.validity.RowIsValid(val_idx)) {
			// Value is NULL
			val = yyjson_mut_null(doc);
		} else if (value_type == LogicalTypeId::JSON) {
			// JSON is a string_t, but is treated differently
			val = CreateJSONValueFromJSON(doc, (string_t &)values[val_idx]);
		} else {
			// Simply add
			val = CreateJSONValue<T>(doc, values[val_idx]);
		}
		// Add key value pair to object
		D_ASSERT(key != nullptr && val != nullptr);
		yyjson_mut_obj_add(objs[i], key, val);
	}
}

// Forward declaration so we can recurse for nested types
static void AddKeyValue(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                        Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count, bool in_list);

static void AddKeyValueStruct(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                              Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count, bool in_list) {
	// Create empty JSON objects for the structs
	auto nested_objs_ptr = unique_ptr<yyjson_mut_val *[]>(new yyjson_mut_val *[count]);
	auto nested_objs = nested_objs_ptr.get();
	for (idx_t i = 0; i < count; i++) {
		auto doc = in_list ? docs[0] : docs[i];
		nested_objs[i] = yyjson_mut_obj(doc);
	}

	// Fill the JSON objects with the structs/maps
	auto &entries = StructVector::GetEntries(value_v);
	if (value_v.GetType().id() == LogicalTypeId::STRUCT) {
		for (idx_t entry_i = 0; entry_i < entries.size(); entry_i++) {
			auto &name = StructType::GetChildName(value_v.GetType(), entry_i);
			AddKeyValue(info, docs, nested_objs, *info.const_struct_names.at(name), *entries[entry_i], vcount, offset,
			            count, in_list);
		}
	} else {
		D_ASSERT(value_v.GetType().id() == LogicalTypeId::MAP && entries.size() == 2);
		auto &map_key_v = ListVector::GetEntry(*entries[0]);
		auto &map_val_v = ListVector::GetEntry(*entries[1]);
		AddKeyValue(info, docs, nested_objs, map_key_v, map_val_v, vcount, offset, count, in_list);
	}

	VectorData key_data;
	key_v.Orrify(vcount, key_data);
	auto keys = (string_t *)key_data.data;

	VectorData struct_data;
	value_v.Orrify(vcount, struct_data);

	// Add key value pairs to object
	for (idx_t i = 0; i < count; i++) {
		idx_t key_idx = key_data.sel->get_index(i) + offset;
		if (!key_data.validity.RowIsValid(key_idx)) {
			// Key is NULL, we can't add
			continue;
		}
		auto key = CreateJSONValue<string_t>(docs[i], keys[key_idx]);
		yyjson_mut_val *val;
		auto doc = in_list ? docs[0] : docs[i];
		if (!struct_data.validity.RowIsValid(struct_data.sel->get_index(i) + offset)) {
			// Whole struct is NULL
			val = yyjson_mut_null(doc);
		} else {
			val = nested_objs[i];
		}
		// Add key value pair to object
		D_ASSERT(key != nullptr && val != nullptr);
		if (in_list) {
			auto obj = yyjson_mut_obj(doc);
			yyjson_mut_obj_add(obj, key, val);
			yyjson_mut_arr_append(objs[0], obj);
		} else {
			yyjson_mut_obj_add(objs[i], key, val);
		}
	}
}

template <class T>
static void TemplatedAddKeyValueList(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                                     Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count,
                                     bool in_list) {
	VectorData key_data;
	key_v.Orrify(vcount, key_data);
	auto keys = (string_t *)key_data.data;

	VectorData list_data;
	value_v.Orrify(vcount, list_data);
	auto list_entries = (list_entry_t *)list_data.data;

	auto &child_v = ListVector::GetEntry(value_v);
	auto child_count = ListVector::GetListSize(value_v);
	VectorData child_data;
	child_v.Orrify(child_count, child_data);
	auto child_values = (T *)child_data.data;

	// Fill the JSON arrays with the list contents
	const auto value_type = child_v.GetType().id();
	for (idx_t i = 0; i < count; i++) {
		idx_t key_idx = key_data.sel->get_index(i) + offset;
		if (!key_data.validity.RowIsValid(key_idx)) {
			continue;
		}
		auto key = CreateJSONValue<string_t>(docs[i], keys[key_idx]);

		auto doc = in_list ? docs[0] : docs[i];
		yyjson_mut_val *val;

		idx_t list_idx = list_data.sel->get_index(i) + offset;
		if (!list_data.validity.RowIsValid(list_idx)) {
			// Whole list is NULL
			val = yyjson_mut_null(doc);
		} else {
			val = yyjson_mut_arr(doc);
			const auto &entry = list_entries[list_idx];
			for (idx_t child_i = entry.offset; child_i < entry.offset + entry.length; child_i++) {
				idx_t child_val_idx = child_data.sel->get_index(child_i);
				if (!child_data.validity.RowIsValid(child_val_idx)) {
					// Value is NULL
					yyjson_mut_arr_append(val, yyjson_mut_null(doc));
				} else if (value_type == LogicalTypeId::JSON) {
					// JSON is a string_t, but is treated differently
					val = CreateJSONValueFromJSON(doc, (string_t &)child_values[child_val_idx]);
				} else {
					// Simply add
					yyjson_mut_arr_append(val, CreateJSONValue<T>(doc, child_values[child_val_idx]));
				}
			}
		}
		// Add key value pair to object
		D_ASSERT(key != nullptr && val != nullptr);
		if (in_list) {
			auto obj = yyjson_mut_obj(doc);
			yyjson_mut_obj_add(obj, key, val);
			yyjson_mut_arr_append(objs[0], obj);
		} else {
			yyjson_mut_obj_add(objs[i], key, val);
		}
	}
}

static void AddKeyValueStructList(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                                  Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count,
                                  bool in_list) {
}

static void AddKeyValueListList(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                                Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count, bool in_list) {
}

static void AddKeyValueList(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                            Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count, bool in_list) {
	switch (ListType::GetChildType(value_v.GetType()).id()) {
	case LogicalTypeId::BOOLEAN:
		TemplatedAddKeyValueList<bool>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::BIGINT:
		TemplatedAddKeyValueList<int64_t>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedAddKeyValueList<uint64_t>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::DOUBLE:
		TemplatedAddKeyValueList<double>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		TemplatedAddKeyValueList<string_t>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	// TODO: these
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
		AddKeyValueStructList(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::LIST:
		AddKeyValueListList(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	default:
		throw InternalException("Unsupported type arrived at ObjectFunction");
	}
}

static void AddKeyValue(const JSONObjectFunctionData &info, yyjson_mut_doc *docs[], yyjson_mut_val *objs[],
                        Vector &key_v, Vector &value_v, idx_t vcount, idx_t offset, idx_t count, bool in_list) {
	switch (value_v.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		TemplatedAddKeyValue<bool>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::BIGINT:
		TemplatedAddKeyValue<int64_t>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedAddKeyValue<uint64_t>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::DOUBLE:
		TemplatedAddKeyValue<double>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		TemplatedAddKeyValue<string_t>(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
		AddKeyValueStruct(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	case LogicalTypeId::LIST:
		AddKeyValueList(info, docs, objs, key_v, value_v, vcount, offset, count, in_list);
		break;
	default:
		throw InternalException("Unsupported type arrived at ObjectFunction");
	}
}

static void ObjectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONObjectFunctionData &)*func_expr.bind_info;
	// Initialize documents
	const idx_t count = args.size();
	yyjson_mut_doc *docs[STANDARD_VECTOR_SIZE];
	yyjson_mut_val *objs[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		docs[i] = yyjson_mut_doc_new(nullptr);
		yyjson_mut_doc_set_root(docs[i], yyjson_mut_obj(docs[i]));
		objs[i] = docs[i]->root;
	}
	// Loop through key/value pairs
	for (idx_t pair_idx = 0; pair_idx < args.data.size() / 2; pair_idx++) {
		Vector &key_v = args.data[pair_idx * 2];
		Vector &value_v = args.data[pair_idx * 2 + 1];
		AddKeyValue(info, docs, objs, key_v, value_v, count, 0, count, false);
	}
	// Write JSON objects to string
	auto objects = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		objects[i] = StringVector::AddString(result, JSONCommon::WriteVal(docs[i]));
		yyjson_mut_doc_free(docs[i]);
	}
}

CreateScalarFunctionInfo JSONFunctions::GetObjectFunction() {
	auto fun =
	    ScalarFunction("json_object", {}, LogicalType::JSON, ObjectFunction, false, JSONObjectBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
