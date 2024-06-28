//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_executors.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "json_functions.hpp"

namespace duckdb {

struct JSONExecutors {
public:
	//! Single-argument JSON read function, i.e. json_type('[1, 2, 3]')
	template <class T>
	static void UnaryExecute(DataChunk &args, ExpressionState &state, Vector &result,
	                         std::function<T(yyjson_val *, yyjson_alc *, Vector &)> fun) {
		auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
		auto alc = lstate.json_allocator.GetYYAlc();

		auto &inputs = args.data[0];
		UnaryExecutor::Execute<string_t, T>(inputs, result, args.size(), [&](string_t input) {
			auto doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, alc);
			return fun(doc->root, alc, result);
		});
	}

	//! Two-argument JSON read function (with path query), i.e. json_type('[1, 2, 3]', '$[0]')
	template <class T, bool NULL_IF_NULL = true>
	static void BinaryExecute(DataChunk &args, ExpressionState &state, Vector &result,
	                          std::function<T(yyjson_val *, yyjson_alc *, Vector &)> fun) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		const auto &info = func_expr.bind_info->Cast<JSONReadFunctionData>();
		auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
		auto alc = lstate.json_allocator.GetYYAlc();

		auto &inputs = args.data[0];
		if (info.constant) { // Constant path
			const char *ptr = info.ptr;
			const idx_t &len = info.len;
			if (info.path_type == JSONCommon::JSONPathType::REGULAR) {
				UnaryExecutor::ExecuteWithNulls<string_t, T>(
				    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
					    auto doc =
					        JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, lstate.json_allocator.GetYYAlc());
					    auto val = JSONCommon::GetUnsafe(doc->root, ptr, len);
					    if (!val || (NULL_IF_NULL && unsafe_yyjson_is_null(val))) {
						    mask.SetInvalid(idx);
						    return T {};
					    } else {
						    return fun(val, alc, result);
					    }
				    });
			} else {
				D_ASSERT(info.path_type == JSONCommon::JSONPathType::WILDCARD);
				vector<yyjson_val *> vals;
				UnaryExecutor::Execute<string_t, list_entry_t>(inputs, result, args.size(), [&](string_t input) {
					vals.clear();

					auto doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, lstate.json_allocator.GetYYAlc());
					JSONCommon::GetWildcardPath(doc->root, ptr, len, vals);

					auto current_size = ListVector::GetListSize(result);
					auto new_size = current_size + vals.size();
					if (ListVector::GetListCapacity(result) < new_size) {
						ListVector::Reserve(result, new_size);
					}

					auto &child_entry = ListVector::GetEntry(result);
					auto child_vals = FlatVector::GetData<T>(child_entry);
					auto &child_validity = FlatVector::Validity(child_entry);
					for (idx_t i = 0; i < vals.size(); i++) {
						auto &val = vals[i];
						D_ASSERT(val != nullptr); // Wildcard extract shouldn't give back nullptrs
						if (NULL_IF_NULL && unsafe_yyjson_is_null(val)) {
							child_validity.SetInvalid(current_size + i);
						} else {
							child_vals[current_size + i] = fun(val, alc, result);
						}
					}

					ListVector::SetListSize(result, new_size);

					return list_entry_t {current_size, vals.size()};
				});
			}
		} else { // Columnref path
			D_ASSERT(info.path_type == JSONCommon::JSONPathType::REGULAR);
			auto &paths = args.data[1];
			BinaryExecutor::ExecuteWithNulls<string_t, string_t, T>(
			    inputs, paths, result, args.size(), [&](string_t input, string_t path, ValidityMask &mask, idx_t idx) {
				    auto doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, lstate.json_allocator.GetYYAlc());
				    auto val = JSONCommon::Get(doc->root, path);
				    if (!val || unsafe_yyjson_is_null(val)) {
					    mask.SetInvalid(idx);
					    return T {};
				    } else {
					    return fun(val, alc, result);
				    }
			    });
		}
		if (args.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	//! JSON read function with list of path queries, i.e. json_type('[1, 2, 3]', ['$[0]', '$[1]'])
	template <class T, bool NULL_IF_NULL = true>
	static void ExecuteMany(DataChunk &args, ExpressionState &state, Vector &result,
	                        std::function<T(yyjson_val *, yyjson_alc *, Vector &)> fun) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		const auto &info = func_expr.bind_info->Cast<JSONReadManyFunctionData>();
		auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
		auto alc = lstate.json_allocator.GetYYAlc();
		D_ASSERT(info.ptrs.size() == info.lens.size());

		const auto count = args.size();
		const idx_t num_paths = info.ptrs.size();
		const idx_t list_size = count * num_paths;

		UnifiedVectorFormat input_data;
		auto &input_vector = args.data[0];
		input_vector.ToUnifiedFormat(count, input_data);
		auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);

		ListVector::Reserve(result, list_size);
		auto list_entries = FlatVector::GetData<list_entry_t>(result);
		auto &list_validity = FlatVector::Validity(result);

		auto &child = ListVector::GetEntry(result);
		auto child_data = FlatVector::GetData<T>(child);
		auto &child_validity = FlatVector::Validity(child);

		idx_t offset = 0;
		yyjson_val *val;
		for (idx_t i = 0; i < count; i++) {
			auto idx = input_data.sel->get_index(i);
			if (!input_data.validity.RowIsValid(idx)) {
				list_validity.SetInvalid(i);
				continue;
			}

			auto doc = JSONCommon::ReadDocument(inputs[idx], JSONCommon::READ_FLAG, lstate.json_allocator.GetYYAlc());
			for (idx_t path_i = 0; path_i < num_paths; path_i++) {
				auto child_idx = offset + path_i;
				val = JSONCommon::GetUnsafe(doc->root, info.ptrs[path_i], info.lens[path_i]);
				if (!val || (NULL_IF_NULL && unsafe_yyjson_is_null(val))) {
					child_validity.SetInvalid(child_idx);
				} else {
					child_data[child_idx] = fun(val, alc, child);
				}
			}

			list_entries[i].offset = offset;
			list_entries[i].length = num_paths;
			offset += num_paths;
		}
		ListVector::SetListSize(result, offset);

		if (args.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}
};

} // namespace duckdb
