#include "json_executors.hpp"

namespace duckdb {

static inline bool JSONContains(yyjson_val *haystack, yyjson_val *needle);

static inline bool JSONArrayContainsArray(yyjson_val *haystack, yyjson_val *needle) {
	D_ASSERT(yyjson_get_tag(haystack) == (YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE) &&
	         yyjson_get_tag(needle) == (YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE));
	size_t needle_idx, needle_max, haystack_idx, haystack_max;
	yyjson_val *needle_child, *haystack_child;
	yyjson_arr_foreach(needle, needle_idx, needle_max, needle_child) {
		bool found = false;
		yyjson_arr_foreach(haystack, haystack_idx, haystack_max, haystack_child) {
			if (JSONContains(haystack_child, needle_child)) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

static inline bool JSONObjectContainsObject(yyjson_val *haystack, yyjson_val *needle) {
	D_ASSERT(yyjson_get_tag(haystack) == (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE) &&
	         yyjson_get_tag(needle) == (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE));
	size_t idx, max;
	yyjson_val *key, *needle_child;
	yyjson_obj_foreach(needle, idx, max, key, needle_child) {
		auto haystack_child = yyjson_obj_get(haystack, yyjson_get_str(key));
		if (!haystack_child || !JSONContains(haystack_child, needle_child)) {
			return false;
		}
	}
	return true;
}

static inline bool JSONContains(yyjson_val *haystack, yyjson_val *needle) {
	if (yyjson_equals(haystack, needle)) {
		return true;
	}

	switch (yyjson_get_tag(haystack)) {
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE: {
		if (yyjson_get_tag(needle) == (YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE) &&
		    JSONArrayContainsArray(haystack, needle)) {
			return true;
		}
		size_t idx, max;
		yyjson_val *child_haystack;
		yyjson_arr_foreach(haystack, idx, max, child_haystack) {
			if (JSONContains(child_haystack, needle)) {
				return true;
			}
		}
		break;
	}
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE: {
		if (yyjson_get_tag(needle) == (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE) &&
		    JSONObjectContainsObject(haystack, needle)) {
			return true;
		}
		size_t idx, max;
		yyjson_val *key, *child_haystack;
		yyjson_obj_foreach(haystack, idx, max, key, child_haystack) {
			if (JSONContains(child_haystack, needle)) {
				return true;
			}
		}
		break;
	}
	default:
		break;
	}
	return false;
}

static void JSONContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);

	auto &haystacks = args.data[0];
	auto &needles = args.data[1];

	if (needles.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto &needle_str = *ConstantVector::GetData<string_t>(needles);
		auto needle_doc =
		    JSONCommon::ReadDocument(needle_str, JSONCommon::READ_FLAG, lstate.json_allocator.GetYYJSONAllocator());
		UnaryExecutor::Execute<string_t, bool>(haystacks, result, args.size(), [&](string_t haystack_str) {
			auto haystack_doc = JSONCommon::ReadDocument(haystack_str, JSONCommon::READ_FLAG,
			                                             lstate.json_allocator.GetYYJSONAllocator());
			return JSONContains(haystack_doc->root, needle_doc->root);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, bool>(
		    haystacks, needles, result, args.size(), [&](string_t haystack_str, string_t needle_str) {
			    auto needle_doc = JSONCommon::ReadDocument(needle_str, JSONCommon::READ_FLAG,
			                                               lstate.json_allocator.GetYYJSONAllocator());
			    auto haystack_doc = JSONCommon::ReadDocument(haystack_str, JSONCommon::READ_FLAG,
			                                                 lstate.json_allocator.GetYYJSONAllocator());
			    return JSONContains(haystack_doc->root, needle_doc->root);
		    });
	}
}

static void GetContainsFunctionInternal(ScalarFunctionSet &set, const LogicalType &lhs, const LogicalType &rhs) {
	set.AddFunction(ScalarFunction({lhs, rhs}, LogicalType::BOOLEAN, JSONContainsFunction, nullptr, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
}

CreateScalarFunctionInfo JSONFunctions::GetContainsFunction() {
	ScalarFunctionSet set("json_contains");
	GetContainsFunctionInternal(set, LogicalType::VARCHAR, LogicalType::VARCHAR);
	GetContainsFunctionInternal(set, LogicalType::VARCHAR, JSONCommon::JSONType());
	GetContainsFunctionInternal(set, JSONCommon::JSONType(), LogicalType::VARCHAR);
	GetContainsFunctionInternal(set, JSONCommon::JSONType(), JSONCommon::JSONType());
	// TODO: implement json_contains that accepts path argument as well

	return CreateScalarFunctionInfo(std::move(set));
}

} // namespace duckdb
