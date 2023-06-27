#include "json_structure.hpp"

#include "duckdb/common/enum_util.hpp"
#include "json_executors.hpp"
#include "json_scan.hpp"
#include "json_transform.hpp"

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

JSONStructureNode::JSONStructureNode() : initialized(false) {
}

JSONStructureNode::JSONStructureNode(yyjson_val *key_p, yyjson_val *val_p)
    : key(make_uniq<string>(unsafe_yyjson_get_str(key_p), unsafe_yyjson_get_len(key_p))), initialized(false) {
	D_ASSERT(yyjson_is_str(key_p));
	JSONStructure::ExtractStructure(val_p, *this);
}

JSONStructureNode::JSONStructureNode(JSONStructureNode &&other) noexcept {
	std::swap(key, other.key);
	std::swap(initialized, other.initialized);
	std::swap(descriptions, other.descriptions);
}

JSONStructureNode &JSONStructureNode::operator=(JSONStructureNode &&other) noexcept {
	std::swap(key, other.key);
	std::swap(initialized, other.initialized);
	std::swap(descriptions, other.descriptions);
	return *this;
}

JSONStructureDescription &JSONStructureNode::GetOrCreateDescription(LogicalTypeId type) {
	if (descriptions.empty()) {
		// Empty, just put this type in there
		descriptions.emplace_back(type);
		return descriptions.back();
	}

	if (descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::SQLNULL) {
		// Only a NULL in there, override
		descriptions[0].type = type;
		return descriptions[0];
	}

	if (type == LogicalTypeId::SQLNULL) {
		// 'descriptions' is non-empty, so let's not add NULL
		return descriptions.back();
	}

	// Check if type is already in there or if we can merge numerics
	const auto is_numeric = IsNumeric(type);
	for (auto &description : descriptions) {
		if (type == description.type) {
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

bool JSONStructureNode::ContainsVarchar() const {
	if (descriptions.size() != 1) {
		// We can't refine types if we have more than 1 description (yet), defaults to JSON type for now
		return false;
	}
	auto &description = descriptions[0];
	if (description.type == LogicalTypeId::VARCHAR) {
		return true;
	}
	for (auto &child : description.children) {
		if (child.ContainsVarchar()) {
			return true;
		}
	}

	return false;
}

void JSONStructureNode::InitializeCandidateTypes(const idx_t max_depth, idx_t depth) {
	if (depth >= max_depth) {
		return;
	}
	if (descriptions.size() != 1) {
		// We can't refine types if we have more than 1 description (yet), defaults to JSON type for now
		return;
	}
	auto &description = descriptions[0];
	if (description.type == LogicalTypeId::VARCHAR && !initialized) {
		// We loop through the candidate types and format templates from back to front
		description.candidate_types = {LogicalTypeId::UUID, LogicalTypeId::BIGINT, LogicalTypeId::TIMESTAMP,
		                               LogicalTypeId::DATE, LogicalTypeId::TIME};
	}
	initialized = true;
	for (auto &child : description.children) {
		child.InitializeCandidateTypes(max_depth, depth + 1);
	}
}

void JSONStructureNode::RefineCandidateTypes(yyjson_val *vals[], idx_t count, Vector &string_vector,
                                             ArenaAllocator &allocator, DateFormatMap &date_format_map) {
	if (descriptions.size() != 1) {
		// We can't refine types if we have more than 1 description (yet), defaults to JSON type for now
		return;
	}
	if (!ContainsVarchar()) {
		return;
	}
	auto &description = descriptions[0];
	switch (description.type) {
	case LogicalTypeId::LIST:
		return RefineCandidateTypesArray(vals, count, string_vector, allocator, date_format_map);
	case LogicalTypeId::STRUCT:
		return RefineCandidateTypesObject(vals, count, string_vector, allocator, date_format_map);
	case LogicalTypeId::VARCHAR:
		return RefineCandidateTypesString(vals, count, string_vector, date_format_map);
	default:
		return;
	}
}

void JSONStructureNode::RefineCandidateTypesArray(yyjson_val *vals[], idx_t count, Vector &string_vector,
                                                  ArenaAllocator &allocator, DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::LIST);
	auto &desc = descriptions[0];
	D_ASSERT(desc.children.size() == 1);
	auto &child = desc.children[0];

	idx_t total_list_size = 0;
	for (idx_t i = 0; i < count; i++) {
		if (vals[i] && !unsafe_yyjson_is_null(vals[i])) {
			D_ASSERT(yyjson_is_arr(vals[i]));
			total_list_size += unsafe_yyjson_get_len(vals[i]);
		}
	}

	idx_t offset = 0;
	auto child_vals =
	    reinterpret_cast<yyjson_val **>(allocator.AllocateAligned(total_list_size * sizeof(yyjson_val *)));

	size_t idx, max;
	yyjson_val *child_val;
	for (idx_t i = 0; i < count; i++) {
		if (vals[i] && !unsafe_yyjson_is_null(vals[i])) {
			yyjson_arr_foreach(vals[i], idx, max, child_val) {
				child_vals[offset++] = child_val;
			}
		}
	}
	child.RefineCandidateTypes(child_vals, total_list_size, string_vector, allocator, date_format_map);
}

void JSONStructureNode::RefineCandidateTypesObject(yyjson_val *vals[], idx_t count, Vector &string_vector,
                                                   ArenaAllocator &allocator, DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::STRUCT);
	auto &desc = descriptions[0];

	const idx_t child_count = desc.children.size();
	vector<yyjson_val **> child_vals;
	child_vals.reserve(child_count);
	for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
		child_vals.emplace_back(
		    reinterpret_cast<yyjson_val **>(allocator.AllocateAligned(count * sizeof(yyjson_val *))));
	}

	idx_t found_key_count;
	auto found_keys = reinterpret_cast<bool *>(allocator.AllocateAligned(sizeof(bool) * child_count));

	const auto &key_map = desc.key_map;
	size_t idx, max;
	yyjson_val *child_key, *child_val;
	for (idx_t i = 0; i < count; i++) {
		if (vals[i] && !unsafe_yyjson_is_null(vals[i])) {
			found_key_count = 0;
			memset(found_keys, false, child_count);

			D_ASSERT(yyjson_is_obj(vals[i]));
			yyjson_obj_foreach(vals[i], idx, max, child_key, child_val) {
				D_ASSERT(yyjson_is_str(child_key));
				auto key_ptr = unsafe_yyjson_get_str(child_key);
				auto key_len = unsafe_yyjson_get_len(child_key);
				auto it = key_map.find({key_ptr, key_len});
				D_ASSERT(it != key_map.end());
				const auto child_idx = it->second;
				child_vals[child_idx][i] = child_val;
				found_keys[child_idx] = true;
				found_key_count++;
			}

			if (found_key_count != child_count) {
				// Set child val to nullptr so recursion doesn't break
				for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
					if (!found_keys[child_idx]) {
						child_vals[child_idx][i] = nullptr;
					}
				}
			}
		} else {
			for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
				child_vals[child_idx][i] = nullptr;
			}
		}
	}

	for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
		desc.children[child_idx].RefineCandidateTypes(child_vals[child_idx], count, string_vector, allocator,
		                                              date_format_map);
	}
}

void JSONStructureNode::RefineCandidateTypesString(yyjson_val *vals[], idx_t count, Vector &string_vector,
                                                   DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::VARCHAR);
	if (descriptions[0].candidate_types.empty()) {
		return;
	}
	static JSONTransformOptions OPTIONS;
	JSONTransform::GetStringVector(vals, count, LogicalType::SQLNULL, string_vector, OPTIONS);
	EliminateCandidateTypes(count, string_vector, date_format_map);
}

void JSONStructureNode::EliminateCandidateTypes(idx_t count, Vector &string_vector, DateFormatMap &date_format_map) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::VARCHAR);
	auto &description = descriptions[0];
	auto &candidate_types = description.candidate_types;
	while (true) {
		if (candidate_types.empty()) {
			return;
		}
		const auto type = candidate_types.back();
		Vector result_vector(type, count);
		if (date_format_map.HasFormats(type)) {
			auto &formats = date_format_map.GetCandidateFormats(type);
			if (EliminateCandidateFormats(count, string_vector, result_vector, formats)) {
				return;
			} else {
				candidate_types.pop_back();
			}
		} else {
			string error_message;
			if (!VectorOperations::DefaultTryCast(string_vector, result_vector, count, &error_message, true)) {
				candidate_types.pop_back();
			} else {
				return;
			}
		}
	}
}

template <class OP, class T>
bool TryParse(Vector &string_vector, StrpTimeFormat &format, const idx_t count) {
	const auto strings = FlatVector::GetData<string_t>(string_vector);
	const auto &validity = FlatVector::Validity(string_vector);

	T result;
	string error_message;
	if (validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			if (!OP::template Operation<T>(format, strings[i], result, error_message)) {
				return false;
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			if (validity.RowIsValid(i)) {
				if (!OP::template Operation<T>(format, strings[i], result, error_message)) {
					return false;
				}
			}
		}
	}
	return true;
}

bool JSONStructureNode::EliminateCandidateFormats(idx_t count, Vector &string_vector, Vector &result_vector,
                                                  vector<StrpTimeFormat> &formats) {
	D_ASSERT(descriptions.size() == 1 && descriptions[0].type == LogicalTypeId::VARCHAR);
	const auto type = result_vector.GetType().id();
	for (idx_t i = formats.size(); i != 0; i--) {
		idx_t actual_index = i - 1;
		auto &format = formats[actual_index];
		bool success;
		switch (type) {
		case LogicalTypeId::DATE:
			success = TryParse<TryParseDate, date_t>(string_vector, format, count);
			break;
		case LogicalTypeId::TIMESTAMP:
			success = TryParse<TryParseTimeStamp, timestamp_t>(string_vector, format, count);
			break;
		default:
			throw InternalException("No date/timestamp formats for %s", EnumUtil::ToString(type));
		}
		if (success) {
			while (formats.size() > i) {
				formats.pop_back();
			}
			return true;
		}
	}
	return false;
}

JSONStructureDescription::JSONStructureDescription(LogicalTypeId type_p) : type(type_p) {
}

JSONStructureDescription::JSONStructureDescription(JSONStructureDescription &&other) noexcept {
	std::swap(type, other.type);
	std::swap(key_map, other.key_map);
	std::swap(children, other.children);
	std::swap(candidate_types, other.candidate_types);
}

JSONStructureDescription &JSONStructureDescription::operator=(JSONStructureDescription &&other) noexcept {
	std::swap(type, other.type);
	std::swap(key_map, other.key_map);
	std::swap(children, other.children);
	std::swap(candidate_types, other.candidate_types);
	return *this;
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
	JSONKey temp_key {unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key)};
	auto it = key_map.find(temp_key);
	if (it == key_map.end()) { // Didn't find, create a new child
		child_idx = children.size();
		children.emplace_back(key, val);
		const auto &persistent_key_string = children.back().key;
		JSONKey new_key {persistent_key_string->c_str(), persistent_key_string->length()};
		key_map.emplace(new_key, child_idx);
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
	node.GetOrCreateDescription(JSONCommon::ValTypeToLogicalTypeId(val));
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
	if (desc.children.empty()) {
		// Empty struct - let's do JSON instead
		return yyjson_mut_str(doc, JSONCommon::JSON_TYPE_NAME);
	}

	auto obj = yyjson_mut_obj(doc);
	for (auto &child : desc.children) {
		D_ASSERT(child.key);
		yyjson_mut_obj_add(obj, yyjson_mut_strn(doc, child.key->c_str(), child.key->length()),
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
		return yyjson_mut_str(doc, EnumUtil::ToChars(desc.type));
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

ScalarFunctionSet JSONFunctions::GetStructureFunction() {
	ScalarFunctionSet set("json_structure");
	GetStructureFunctionInternal(set, LogicalType::VARCHAR);
	GetStructureFunctionInternal(set, JSONCommon::JSONType());
	return set;
}

static LogicalType StructureToTypeArray(ClientContext &context, const JSONStructureNode &node, const idx_t max_depth,
                                        idx_t depth) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::LIST);
	const auto &desc = node.descriptions[0];
	D_ASSERT(desc.children.size() == 1);

	return LogicalType::LIST(JSONStructure::StructureToType(context, desc.children[0], max_depth, depth + 1));
}

static LogicalType StructureToTypeObject(ClientContext &context, const JSONStructureNode &node, const idx_t max_depth,
                                         idx_t depth) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::STRUCT);
	auto &desc = node.descriptions[0];
	if (desc.children.empty()) {
		// Empty struct - let's do JSON instead
		return JSONCommon::JSONType();
	}

	child_list_t<LogicalType> child_types;
	child_types.reserve(desc.children.size());
	for (auto &child : desc.children) {
		D_ASSERT(child.key);
		child_types.emplace_back(*child.key, JSONStructure::StructureToType(context, child, max_depth, depth + 1));
	}
	return LogicalType::STRUCT(child_types);
}

static LogicalType StructureToTypeString(const JSONStructureNode &node) {
	D_ASSERT(node.descriptions.size() == 1 && node.descriptions[0].type == LogicalTypeId::VARCHAR);
	auto &desc = node.descriptions[0];
	if (desc.candidate_types.empty()) {
		return LogicalTypeId::VARCHAR;
	}
	return desc.candidate_types.back();
}

LogicalType JSONStructure::StructureToType(ClientContext &context, const JSONStructureNode &node, const idx_t max_depth,
                                           idx_t depth) {
	if (depth >= max_depth) {
		return JSONCommon::JSONType();
	}
	if (node.descriptions.empty()) {
		return JSONCommon::JSONType();
	}
	if (node.descriptions.size() != 1) { // Inconsistent types, so we resort to JSON
		return JSONCommon::JSONType();
	}
	auto &desc = node.descriptions[0];
	D_ASSERT(desc.type != LogicalTypeId::INVALID);
	switch (desc.type) {
	case LogicalTypeId::LIST:
		return StructureToTypeArray(context, node, max_depth, depth);
	case LogicalTypeId::STRUCT:
		return StructureToTypeObject(context, node, max_depth, depth);
	case LogicalTypeId::VARCHAR:
		return StructureToTypeString(node);
	case LogicalTypeId::SQLNULL:
		return JSONCommon::JSONType();
	case LogicalTypeId::UBIGINT:
		return LogicalTypeId::BIGINT; // We prefer not to return UBIGINT in our type auto-detection
	default:
		return desc.type;
	}
}

} // namespace duckdb
