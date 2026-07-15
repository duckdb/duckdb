#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Append a character to a JSON Pointer, escaping ~ and / as required by RFC 6901
static void AppendPointerCharacter(string &result, char c) {
	if (c == '~') {
		result += "~0";
	} else if (c == '/') {
		result += "~1";
	} else {
		result += c;
	}
}

//! Convert a JSONPath ($.key[0].nested) to a JSON Pointer (/key/0/nested).
//! Paths starting with '/' are returned as-is. Bare key names are wrapped as /key.
//! Array indexes may be # (the array length) or #-N (the N-th element from the end),
//! both resolved against the document. Returns false if the path cannot be resolved.
static bool ConvertToJsonPointer(const string_t &path_str, yyjson_mut_doc *doc, string &result) {
	auto ptr = path_str.GetData();
	auto len = path_str.GetSize();
	result = "";
	if (len == 0) {
		return true;
	}
	// Already a JSON Pointer
	if (*ptr == '/') {
		result = string(ptr, len);
		return true;
	}
	// Bare key name
	if (*ptr != '$') {
		result = "/";
		for (idx_t i = 0; i < len; i++) {
			AppendPointerCharacter(result, ptr[i]);
		}
		return true;
	}
	// JSONPath: convert $.key[0].nested to /key/0/nested
	idx_t i = 1; // Skip '$'
	while (i < len) {
		auto c = ptr[i++];
		if (c == '.') {
			result += '/';
			if (i < len && ptr[i] == '"') {
				i++; // Skip opening quote
				while (i < len && ptr[i] != '"') {
					auto k = ptr[i];
					if (k == '\\' && i + 1 < len) {
						i++; // \" and \\ address the escaped character itself
						k = ptr[i];
					}
					AppendPointerCharacter(result, k);
					i++;
				}
				if (i < len) {
					i++; // Skip closing quote
				}
			} else {
				while (i < len && ptr[i] != '.' && ptr[i] != '[') {
					AppendPointerCharacter(result, ptr[i]);
					i++;
				}
			}
		} else if (c == '[') {
			result += '/';
			if (i < len && ptr[i] == '#') {
				i++; // Skip '#'
				idx_t offset = 0;
				if (i < len && ptr[i] == '-') {
					i++; // Skip '-'
					bool has_digits = false;
					while (i < len && ptr[i] >= '0' && ptr[i] <= '9') {
						if (offset > (NumericLimits<idx_t>::Maximum() - 9) / 10) {
							return false; // The offset would overflow, it cannot be a valid index
						}
						offset = offset * 10 + static_cast<idx_t>(ptr[i] - '0');
						has_digits = true;
						i++;
					}
					if (!has_digits) {
						return false;
					}
				}
				if (i < len && ptr[i] == ']') {
					i++; // Skip ']'
				}
				// Resolve # against the length of the array at the path so far.
				// result still ends with '/', so the parent is everything before it.
				auto parent = yyjson_mut_doc_ptr_getx(doc, result.c_str(), result.size() - 1, nullptr, nullptr);
				if (!parent || !yyjson_mut_is_arr(parent)) {
					return false;
				}
				auto arr_len = yyjson_mut_arr_size(parent);
				if (offset > arr_len) {
					return false;
				}
				result += to_string(arr_len - offset);
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
	return true;
}

//! Set a value at a path in a JSON document (create if missing, overwrite if exists)
static void JsonSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, [&](string_t doc_str, string_t path_str, string_t val_str) {
		    auto doc = JSONCommon::ReadDocument(doc_str, JSONCommon::READ_FLAG, alc);
		    auto mut_doc = yyjson_doc_mut_copy(doc, alc);

		    auto val_doc = JSONCommon::ReadDocument(val_str, JSONCommon::READ_FLAG, alc);
		    auto new_val = yyjson_val_mut_copy(mut_doc, val_doc->root);

		    string pointer;
		    if (ConvertToJsonPointer(path_str, mut_doc, pointer)) {
			    // Try set first (overwrites existing, creates missing).
			    // Fall back to add for cases set cannot handle (e.g. appending with /-).
			    if (!yyjson_mut_doc_ptr_setx(mut_doc, pointer.c_str(), pointer.size(), new_val, true, nullptr,
			                                 nullptr)) {
				    yyjson_mut_doc_ptr_addx(mut_doc, pointer.c_str(), pointer.size(), new_val, true, nullptr, nullptr);
			    }
		    }

		    auto root = yyjson_mut_doc_get_root(mut_doc);
		    return JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
	    });

	JSONAllocator::AddBuffer(result, alc);
}

//! Insert a value at a path in a JSON document (no-op if a value already exists at the path)
static void JsonInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, [&](string_t doc_str, string_t path_str, string_t val_str) {
		    auto doc = JSONCommon::ReadDocument(doc_str, JSONCommon::READ_FLAG, alc);
		    auto mut_doc = yyjson_doc_mut_copy(doc, alc);

		    string pointer;
		    // Only insert if the path resolves and there is no value at it yet
		    if (ConvertToJsonPointer(path_str, mut_doc, pointer) &&
		        !yyjson_mut_doc_ptr_getx(mut_doc, pointer.c_str(), pointer.size(), nullptr, nullptr)) {
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

		    string pointer;
		    if (ConvertToJsonPointer(path_str, mut_doc, pointer)) {
			    // Returns the old value if the path exists, NULL otherwise (nothing is replaced)
			    yyjson_mut_doc_ptr_replacex(mut_doc, pointer.c_str(), pointer.size(), new_val, nullptr, nullptr);
		    }

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

		    string pointer;
		    if (ConvertToJsonPointer(path_str, mut_doc, pointer)) {
			    // Returns the removed value, or NULL if the path does not exist (nothing is removed)
			    yyjson_mut_doc_ptr_removex(mut_doc, pointer.c_str(), pointer.size(), nullptr, nullptr);
		    }

		    // Removing the root leaves no document behind
		    auto root = yyjson_mut_doc_get_root(mut_doc);
		    if (!root) {
			    return nullopt;
		    }
		    return JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
	    });

	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetSetFunction() {
	ScalarFunction fun("json_set", {LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON()},
	                   LogicalType::JSON(), JsonSetFunction, nullptr, nullptr, JSONFunctionLocalState::Init);
	return ScalarFunctionSet(fun);
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
