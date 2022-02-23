#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Forward declaration for recursion
static inline yyjson_mut_val *GetConsistentArrayStructure(const vector<yyjson_mut_val *> &elem_structures,
                                                          yyjson_mut_doc *structure_doc);

static inline yyjson_mut_val *GetConsistentArrayStructureArray(const vector<yyjson_mut_val *> &elem_structures,
                                                               yyjson_mut_doc *structure_doc) {
	yyjson_mut_val *val;
	yyjson_mut_arr_iter iter;
	vector<yyjson_mut_val *> nested_elem_structures;
	for (const auto &elem_structure : elem_structures) {
		yyjson_mut_arr_iter_init(elem_structure, &iter);
		while ((val = yyjson_mut_arr_iter_next(&iter))) {
			nested_elem_structures.push_back(val);
		}
	}
	auto result = yyjson_mut_arr(structure_doc);
	yyjson_mut_arr_append(result, GetConsistentArrayStructure(nested_elem_structures, structure_doc));
	return result;
}

static inline yyjson_mut_val *GetConsistentArrayStructureObject(const vector<yyjson_mut_val *> &elem_structures,
                                                                yyjson_mut_doc *structure_doc) {
	yyjson_mut_val *key, *val;
	yyjson_mut_obj_iter iter;
	vector<string> key_insert_order;
	unordered_map<string, vector<yyjson_mut_val *>> key_values;
	for (const auto &elem_structure : elem_structures) {
		yyjson_mut_obj_iter_init(elem_structure, &iter);
		while ((key = yyjson_mut_obj_iter_next(&iter))) {
			auto key_string = yyjson_mut_get_str(key);
			val = yyjson_mut_obj_iter_get_val(key);
			if (key_values.find(key_string) == key_values.end()) {
				key_insert_order.emplace_back(key_string);
			}
			key_values[key_string].push_back(val);
		}
	}
	auto result = yyjson_mut_obj(structure_doc);
	for (const auto &key_string : key_insert_order) {
		key = yyjson_mut_strncpy(structure_doc, key_string.c_str(), key_string.size());
		val = GetConsistentArrayStructure(key_values.at(key_string), structure_doc);
		yyjson_mut_obj_add(result, key, val);
	}
	return result;
}

static inline yyjson_mut_val *GetMaxTypeVal(yyjson_mut_val *a, yyjson_mut_val *b) {
	auto a_type = yyjson_mut_get_type(a);
	auto b_type = yyjson_mut_get_type(b);
	if (a_type == YYJSON_TYPE_NULL) {
		return b;
	} else if (b_type == YYJSON_TYPE_NULL) {
		return a;
	} else if (a_type == YYJSON_TYPE_ARR) {
		if (b_type != YYJSON_TYPE_ARR) {
			throw InvalidInputException("Inconsistent JSON structure");
		} else {
			return a;
		}
	} else if (a_type == YYJSON_TYPE_OBJ) {
		if (b_type != YYJSON_TYPE_OBJ) {
			throw InvalidInputException("Inconsistent JSON structure");
		} else {
			return a;
		}
	} else if (a_type == YYJSON_TYPE_STR) {
		return a;
	} else if (b_type == YYJSON_TYPE_STR) {
		return b;
	}
	auto a_subtype = yyjson_mut_get_subtype(a);
	auto b_subtype = yyjson_mut_get_subtype(b);
	if (a_subtype == YYJSON_SUBTYPE_REAL) {
		return a;
	} else if (b_subtype == YYJSON_SUBTYPE_REAL) {
		return b;
	} else if (a_subtype == YYJSON_SUBTYPE_SINT) {
		return a;
	} else if (b_subtype == YYJSON_SUBTYPE_SINT) {
		return b;
	} else if (a_subtype == YYJSON_SUBTYPE_UINT) {
		return a;
	} else if (b_subtype == YYJSON_SUBTYPE_UINT) {
		return b;
	}
	D_ASSERT(a_type == YYJSON_TYPE_BOOL && b_type == YYJSON_TYPE_BOOL);
	return a;
}

//! Arrays must have compatible types
static inline yyjson_mut_val *GetConsistentArrayStructure(const vector<yyjson_mut_val *> &elem_structures,
                                                          yyjson_mut_doc *structure_doc) {
	if (elem_structures.empty()) {
		return yyjson_mut_null(structure_doc);
	}
	auto val = elem_structures[0];
	for (idx_t i = 1; i < elem_structures.size(); i++) {
		val = GetMaxTypeVal(val, elem_structures[i]);
	}
	switch (yyjson_mut_get_type(val)) {
	case YYJSON_TYPE_ARR:
		return GetConsistentArrayStructureArray(elem_structures, structure_doc);
	case YYJSON_TYPE_OBJ:
		return GetConsistentArrayStructureObject(elem_structures, structure_doc);
	default:
		return val;
	}
}

//! Forward declaration for recursion
static inline yyjson_mut_val *BuildStructure(yyjson_val *val, yyjson_mut_doc *structure_doc);

static inline yyjson_mut_val *BuildStructureArray(yyjson_val *arr, yyjson_mut_doc *structure_doc) {
	vector<yyjson_mut_val *> elem_structures;
	// Iterate over array
	yyjson_val *val;
	yyjson_arr_iter iter;
	yyjson_arr_iter_init(arr, &iter);
	while ((val = yyjson_arr_iter_next(&iter))) {
		elem_structures.push_back(BuildStructure(val, structure_doc));
	}
	// Array is consistent if it is empty, or if all its elements have the same type (NULL is fine too)
	// If the array has nested types, we need to verify that these match too
	// We combine the structures in the array and try to return a structure without nulls
	auto result = yyjson_mut_arr(structure_doc);
	yyjson_mut_arr_append(result, GetConsistentArrayStructure(elem_structures, structure_doc));
	return result;
}

static inline yyjson_mut_val *BuildStructureObject(yyjson_val *obj, yyjson_mut_doc *structure_doc) {
	auto result = yyjson_mut_obj(structure_doc);
	// Iterate over object
	yyjson_val *key, *val;
	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);
	while ((key = yyjson_obj_iter_next(&iter))) {
		val = yyjson_obj_iter_get_val(key);
		yyjson_mut_obj_add(result, yyjson_val_mut_copy(structure_doc, key), BuildStructure(val, structure_doc));
	}
	return result;
}

static inline yyjson_mut_val *BuildStructure(yyjson_val *val, yyjson_mut_doc *structure_doc) {
	switch (yyjson_get_type(val)) {
	case YYJSON_TYPE_ARR:
		return BuildStructureArray(val, structure_doc);
	case YYJSON_TYPE_OBJ:
		return BuildStructureObject(val, structure_doc);
	default:
		return yyjson_val_mut_copy(structure_doc, val);
	}
}

//! Forward declaration for recursion
static inline yyjson_mut_val *ConvertStructure(yyjson_mut_val *val, yyjson_mut_doc *structure_doc);

static inline yyjson_mut_val *ConvertStructureArray(yyjson_mut_val *arr, yyjson_mut_doc *structure_doc) {
	D_ASSERT(yyjson_mut_arr_size(arr) == 1);
	yyjson_mut_arr_insert(arr, ConvertStructure(yyjson_mut_arr_remove_first(arr), structure_doc), 0);
	return arr;
}

static inline yyjson_mut_val *ConvertStructureObject(yyjson_mut_val *obj, yyjson_mut_doc *structure_doc) {
	yyjson_mut_val *key;
	yyjson_mut_obj_iter iter;
	yyjson_mut_obj_iter_init(obj, &iter);
	while ((key = yyjson_mut_obj_iter_next(&iter))) {
		yyjson_mut_obj_replace(obj, key, ConvertStructure(yyjson_mut_obj_iter_get_val(key), structure_doc));
	}
	return obj;
}

//! Convert structure to contain type strings
static inline yyjson_mut_val *ConvertStructure(yyjson_mut_val *val, yyjson_mut_doc *structure_doc) {
	switch (yyjson_mut_get_type(val)) {
	case YYJSON_TYPE_ARR:
		return ConvertStructureArray(val, structure_doc);
	case YYJSON_TYPE_OBJ:
		return ConvertStructureObject(val, structure_doc);
	default:
		return yyjson_mut_str(structure_doc, JSONCommon::ValTypeToString<yyjson_mut_val>(val));
	}
}

static inline bool Structure(yyjson_val *val, string_t &result_val, Vector &result) {
	auto structure_doc = JSONCommon::CreateDocument();
	auto structure = BuildStructure(val, *structure_doc);
	yyjson_mut_doc_set_root(*structure_doc, ConvertStructure(structure, *structure_doc));
	result_val = JSONCommon::WriteVal(*structure_doc, result);
	return true;
}

static void StructureFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::UnaryExecute<string_t>(args, state, result, Structure);
}

CreateScalarFunctionInfo JSONFunctions::GetStructureFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_structure", {LogicalType::JSON}, LogicalType::JSON,
	                                               StructureFunction, false, nullptr, nullptr, nullptr));
}

} // namespace duckdb
