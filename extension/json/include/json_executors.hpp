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
	template <class T>
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
			UnaryExecutor::ExecuteWithNulls<string_t, T>(
			    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
				    auto doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, lstate.json_allocator.GetYYAlc());
				    auto val = JSONCommon::GetPointerUnsafe<yyjson_val>(doc->root, ptr, len);
				    if (!val || unsafe_yyjson_is_null(val)) {
					    mask.SetInvalid(idx);
					    return T {};
				    } else {
					    return fun(val, alc, result);
				    }
			    });
		} else { // Columnref path
			auto &paths = args.data[1];
			BinaryExecutor::ExecuteWithNulls<string_t, string_t, T>(
			    inputs, paths, result, args.size(), [&](string_t input, string_t path, ValidityMask &mask, idx_t idx) {
				    auto doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, lstate.json_allocator.GetYYAlc());
				    auto val = JSONCommon::GetPointer<yyjson_val>(doc->root, path);
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
	template <class T>
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
		auto inputs = (string_t *)input_data.data;

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
				val = JSONCommon::GetPointerUnsafe<yyjson_val>(doc->root, info.ptrs[path_i], info.lens[path_i]);
				if (!val || unsafe_yyjson_is_null(val)) {
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
