#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Convert a JSONPath ($.key[0].nested) to a JSON Pointer (/key/0/nested).
//! Paths starting with '/' are returned as-is. Bare key names are wrapped as /key.
static string ConvertToJsonPointer(const string_t &path_str) {
	auto ptr = path_str.GetData();
	auto len = path_str.GetSize();
	if (len == 0) {
		return "";
	}
	// Already a JSON Pointer
	if (*ptr == '/') {
		return string(ptr, len);
	}
	// Bare key name
	if (*ptr != '$') {
		string result = "/";
		for (idx_t i = 0; i < len; i++) {
			if (ptr[i] == '~') {
				result += "~0";
			} else if (ptr[i] == '/') {
				result += "~1";
			} else {
				result += ptr[i];
			}
		}
		return result;
	}
	// JSONPath: convert $.key[0].nested to /key/0/nested
	string result;
	idx_t i = 1; // Skip '$'
	while (i < len) {
		auto c = ptr[i++];
		if (c == '.') {
			result += '/';
			if (i < len && ptr[i] == '"') {
				i++; // Skip opening quote
				while (i < len && ptr[i] != '"') {
					if (ptr[i] == '~') {
						result += "~0";
					} else if (ptr[i] == '/') {
						result += "~1";
					} else {
						result += ptr[i];
					}
					i++;
				}
				if (i < len) {
					i++; // Skip closing quote
				}
			} else {
				while (i < len && ptr[i] != '.' && ptr[i] != '[') {
					if (ptr[i] == '~') {
						result += "~0";
					} else if (ptr[i] == '/') {
						result += "~1";
					} else {
						result += ptr[i];
					}
					i++;
				}
			}
		} else if (c == '[') {
			result += '/';
			if (i < len && ptr[i] == '#') {
				// $[#] -> /- (append)
				result += '-';
				i++; // Skip '#'
				if (i < len && ptr[i] == ']') {
					i++; // Skip ']'
				}
			} else {
				while (i < len && ptr[i] != ']') {
					result += ptr[i];
					i++;
				}
				if (i < len) {
					i++; // Skip ']'
				}
			}
		}
	}
	return result;
}

//! Insert a value at a path in a JSON document (no-op if a value already exists at the path)
static void JsonInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, [&](string_t doc_str, string_t path_str, string_t val_str) {
		    auto doc = JSONCommon::ReadDocument(doc_str, JSONCommon::READ_FLAG, alc);
		    auto mut_doc = yyjson_doc_mut_copy(doc, alc);

		    auto pointer = ConvertToJsonPointer(path_str);

		    // Only insert if there is no value at the path yet
		    if (!yyjson_mut_doc_ptr_getx(mut_doc, pointer.c_str(), pointer.size(), nullptr, nullptr)) {
			    auto val_doc = JSONCommon::ReadDocument(val_str, JSONCommon::READ_FLAG, alc);
			    auto new_val = yyjson_val_mut_copy(mut_doc, val_doc->root);
			    yyjson_mut_doc_ptr_addx(mut_doc, pointer.c_str(), pointer.size(), new_val, true, nullptr, nullptr);
		    }

		    auto root = yyjson_mut_doc_get_root(mut_doc);
		    return JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
	    });

	JSONAllocator::AddBuffer(result, alc);
}

//! Replace the value at a path in a JSON document (no-op if the path does not exist)
static void JsonReplaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, [&](string_t doc_str, string_t path_str, string_t val_str) {
		    auto doc = JSONCommon::ReadDocument(doc_str, JSONCommon::READ_FLAG, alc);
		    auto mut_doc = yyjson_doc_mut_copy(doc, alc);

		    auto val_doc = JSONCommon::ReadDocument(val_str, JSONCommon::READ_FLAG, alc);
		    auto new_val = yyjson_val_mut_copy(mut_doc, val_doc->root);

		    auto pointer = ConvertToJsonPointer(path_str);

		    // Returns the old value if the path exists, NULL otherwise (nothing is replaced)
		    yyjson_mut_doc_ptr_replacex(mut_doc, pointer.c_str(), pointer.size(), new_val, nullptr, nullptr);

		    auto root = yyjson_mut_doc_get_root(mut_doc);
		    return JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
	    });

	JSONAllocator::AddBuffer(result, alc);
}

//! Remove the value at a path in a JSON document (no-op if the path does not exist)
static void JsonRemoveFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, [&](string_t doc_str, string_t path_str) -> optional<string_t> {
		    auto doc = JSONCommon::ReadDocument(doc_str, JSONCommon::READ_FLAG, alc);
		    auto mut_doc = yyjson_doc_mut_copy(doc, alc);

		    auto pointer = ConvertToJsonPointer(path_str);

		    // Returns the removed value, or NULL if the path does not exist (nothing is removed)
		    yyjson_mut_doc_ptr_removex(mut_doc, pointer.c_str(), pointer.size(), nullptr, nullptr);

		    // Removing the root leaves no document behind
		    auto root = yyjson_mut_doc_get_root(mut_doc);
		    if (!root) {
			    return nullopt;
		    }
		    return JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
	    });

	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetInsertFunction() {
	ScalarFunction fun("json_insert", {LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON()},
	                   LogicalType::JSON(), JsonInsertFunction, nullptr, nullptr, JSONFunctionLocalState::Init);
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetReplaceFunction() {
	ScalarFunction fun("json_replace", {LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON()},
	                   LogicalType::JSON(), JsonReplaceFunction, nullptr, nullptr, JSONFunctionLocalState::Init);
	return ScalarFunctionSet(fun);
}

ScalarFunctionSet JSONFunctions::GetRemoveFunction() {
	ScalarFunction fun("json_remove", {LogicalType::JSON(), LogicalType::VARCHAR}, LogicalType::JSON(),
	                   JsonRemoveFunction, nullptr, nullptr, JSONFunctionLocalState::Init);
	return ScalarFunctionSet(fun);
}

} // namespace duckdb
