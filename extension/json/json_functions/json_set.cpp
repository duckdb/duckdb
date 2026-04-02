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

//! Set a value at a path in a JSON document (create if missing, overwrite if exists)
static void JsonSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	const auto count = args.size();

	UnifiedVectorFormat doc_data, path_data, val_data;
	args.data[0].ToUnifiedFormat(count, doc_data);
	args.data[1].ToUnifiedFormat(count, path_data);
	args.data[2].ToUnifiedFormat(count, val_data);
	auto doc_inputs = UnifiedVectorFormat::GetData<string_t>(doc_data);
	auto path_inputs = UnifiedVectorFormat::GetData<string_t>(path_data);
	auto val_inputs = UnifiedVectorFormat::GetData<string_t>(val_data);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto doc_idx = doc_data.sel->get_index(i);
		auto path_idx = path_data.sel->get_index(i);
		auto val_idx = val_data.sel->get_index(i);

		if (!doc_data.validity.RowIsValid(doc_idx) || !path_data.validity.RowIsValid(path_idx) ||
		    !val_data.validity.RowIsValid(val_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto doc = JSONCommon::ReadDocument(doc_inputs[doc_idx], JSONCommon::READ_FLAG, alc);
		auto mut_doc = yyjson_doc_mut_copy(doc, alc);

		auto val_doc = JSONCommon::ReadDocument(val_inputs[val_idx], JSONCommon::READ_FLAG, alc);
		auto new_val = yyjson_val_mut_copy(mut_doc, val_doc->root);

		auto pointer = ConvertToJsonPointer(path_inputs[path_idx]);

		// Try set first (overwrites existing, creates missing).
		// Fall back to add for cases set cannot handle (e.g. appending with /-).
		if (!yyjson_mut_doc_ptr_setx(mut_doc, pointer.c_str(), pointer.size(), new_val, true, nullptr, nullptr)) {
			yyjson_mut_doc_ptr_addx(mut_doc, pointer.c_str(), pointer.size(), new_val, true, nullptr, nullptr);
		}

		auto root = yyjson_mut_doc_get_root(mut_doc);
		if (root) {
			result_data[i] = JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
		} else {
			result_validity.SetInvalid(i);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetSetFunction() {
	ScalarFunction fun("json_set", {LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON()},
	                   LogicalType::JSON(), JsonSetFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init);

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
