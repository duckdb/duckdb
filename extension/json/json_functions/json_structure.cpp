#include "json_structure.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "json_executors.hpp"

namespace duckdb {

static inline bool IsNumeric(LogicalTypeId type) {
	return type == LogicalTypeId::DOUBLE || type == LogicalTypeId::UBIGINT || type == LogicalTypeId::BIGINT;
}

static inline LogicalTypeId MaxNumericType(LogicalTypeId &a, LogicalTypeId &b) {
	D_ASSERT(a != b);
	if (a == LogicalTypeId::DOUBLE || b == LogicalTypeId::DOUBLE) {
		return LogicalTypeId::DOUBLE;
	}
	return LogicalTypeId::BIGINT;
}

JSONStructureNode::JSONStructureNode() {
}

JSONStructureNode::JSONStructureNode(yyjson_val *key_p, yyjson_val *val_p)
    : key(unsafe_yyjson_get_str(key_p), unsafe_yyjson_get_len(key_p)) {
	D_ASSERT(yyjson_is_str(key_p));
	JSONStructure::ExtractStructure(val_p, *this);
}

JSONStructureDescription &JSONStructureNode::GetOrCreateDescription(LogicalTypeId type) {
	// Check if type is already in there or if we can merge numerics
	const auto is_numeric = IsNumeric(type);
	for (auto &description : descriptions) {
		if (type == LogicalTypeId::SQLNULL || type == description.type) {
			return description;
		} else if (is_numeric && IsNumeric(description.type)) {
			description.type = MaxNumericType(type, description.type);
			return description;
		}
	}
	// Type was not there, create a new description
	descriptions.emplace_back(type);
	return descriptions.back();
}

JSONStructureDescription::JSONStructureDescription(LogicalTypeId type_p) : type(type_p) {
}

JSONStructureNode &JSONStructureDescription::GetOrCreateChild() {
	D_ASSERT(type == LogicalTypeId::LIST);
	if (children.empty()) {
		children.emplace_back();
	}
	D_ASSERT(children.size() == 1);
	return children.back();
}

JSONStructureNode &JSONStructureDescription::GetOrCreateChild(yyjson_val *key, yyjson_val *val) {
	D_ASSERT(yyjson_is_str(key));
	// Check if there is already a child with the same key
	idx_t child_idx;
	JSONKey new_obj_key {unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key)};
	auto it = key_map.find(new_obj_key);
	if (it == key_map.end()) { // Didn't find, create a new child
		child_idx = children.size();
		key_map.emplace(new_obj_key, child_idx);
		children.emplace_back(key, val);
	} else { // Found it
		child_idx = it->second;
		JSONStructure::ExtractStructure(val, children[child_idx]);
	}
	return children[child_idx];
}

static inline void ExtractStructureArray(yyjson_val *arr, JSONStructureNode &node) {
	D_ASSERT(yyjson_is_arr(arr));
	auto &description = node.GetOrCreateDescription(LogicalTypeId::LIST);
	auto &child = description.GetOrCreateChild();

	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(arr, idx, max, val) {
		JSONStructure::ExtractStructure(val, child);
	}
}

static inline void ExtractStructureObject(yyjson_val *obj, JSONStructureNode &node) {
	D_ASSERT(yyjson_is_obj(obj));
	auto &description = node.GetOrCreateDescription(LogicalTypeId::STRUCT);

	// Keep track of keys so we can detect duplicates
	json_key_set_t obj_keys;

	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		auto key_ptr = unsafe_yyjson_get_str(key);
		auto key_len = unsafe_yyjson_get_len(key);
		auto insert_result = obj_keys.insert({key_ptr, key_len});
		if (!insert_result.second) {
			JSONCommon::ThrowValFormatError("Duplicate key \"" + string(key_ptr, key_len) + "\" in object %s", obj);
		}
		description.GetOrCreateChild(key, val);
	}
}

static inline void ExtractStructureVal(yyjson_val *val, JSONStructureNode &node) {
	D_ASSERT(!yyjson_is_arr(val) && !yyjson_is_obj(val));
	node.GetOrCreateDescription(JSONCommon::ValTypeToLogicalTypeId<yyjson_val>(val));
}

void JSONStructure::ExtractStructure(yyjson_val *val, JSONStructureNode &node) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
		return ExtractStructureArray(val, node);
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		return ExtractStructureObject(val, node);
	default:
		return ExtractStructureVal(val, node);
	}
}

JSONStructureNode ExtractStructureInternal(yyjson_val *val) {
	JSONStructureNode node;
	JSONStructure::ExtractStructure(val, node);
	return node;
}

//! Forward declaration for recursion
static inline yyjson_mut_val *ConvertStructure(const JSONStructureNode &node, yyjson_mut_doc *doc);

static inline yyjson_mut_val *ConvertStructureArray(const JSONStructureNode &node, yyjson_mut_doc *doc) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::LIST);
	const auto &desc = node.descriptions[0];
	D_ASSERT(desc.children.size() == 1);

	auto arr = yyjson_mut_arr(doc);
	yyjson_mut_arr_append(arr, ConvertStructure(desc.children[0], doc));
	return arr;
}

static inline yyjson_mut_val *ConvertStructureObject(const JSONStructureNode &node, yyjson_mut_doc *doc) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::STRUCT);
	auto &desc = node.descriptions[0];

	auto obj = yyjson_mut_obj(doc);
	for (auto &child : desc.children) {
		D_ASSERT(!child.key.empty());
		yyjson_mut_obj_add(obj, yyjson_mut_strn(doc, child.key.c_str(), child.key.length()),
		                   ConvertStructure(child, doc));
	}
	return obj;
}

static inline yyjson_mut_val *ConvertStructure(const JSONStructureNode &node, yyjson_mut_doc *doc) {
	if (node.descriptions.empty()) {
		return yyjson_mut_str(doc, JSONCommon::TYPE_STRING_NULL);
	}
	if (node.descriptions.size() != 1) { // Inconsistent types, so we resort to JSON
		return yyjson_mut_str(doc, JSONCommon::JSON_TYPE_NAME);
	}
	auto &desc = node.descriptions[0];
	D_ASSERT(desc.type != LogicalTypeId::INVALID);
	switch (desc.type) {
	case LogicalTypeId::LIST:
		return ConvertStructureArray(node, doc);
	case LogicalTypeId::STRUCT:
		return ConvertStructureObject(node, doc);
	default:
		auto type_string = LogicalTypeIdToString(desc.type); // TODO: this requires copying, can be optimized
		return yyjson_mut_strncpy(doc, type_string.c_str(), type_string.length());
	}
}

static inline string_t JSONStructureFunction(yyjson_val *val, yyjson_alc *alc, Vector &result) {
	return JSONCommon::WriteVal<yyjson_mut_val>(
	    ConvertStructure(ExtractStructureInternal(val), yyjson_mut_doc_new(alc)), alc);
}

static void StructureFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryExecute<string_t>(args, state, result, JSONStructureFunction);
}

static void GetStructureFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, JSONCommon::JSONType(), StructureFunction, nullptr, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
}

CreateScalarFunctionInfo JSONFunctions::GetStructureFunction() {
	ScalarFunctionSet set("json_structure");
	GetStructureFunctionInternal(set, LogicalType::VARCHAR);
	GetStructureFunctionInternal(set, JSONCommon::JSONType());
	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
