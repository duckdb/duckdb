//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/variant_path_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/types/variant_writer.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct VariantPathFunction {
	//! Provides unified construction of path based processing functions.
	static ScalarFunctionSet CreateFunctionSet(const Identifier &name, const scalar_function_t &function,
	                                           const LogicalType &return_type, bool path_optional = true,
	                                           init_local_state_t init_state = nullptr);

	//! Executes scalar VARIANT functions that accept an optional constant path argument.
	//! The function may be called as f(VARIANT), f(VARIANT, VARCHAR), f(VARIANT, VARCHAR[]) or f(VARIANT, BIGINT).
	//! The callback `write_fn` writes the result for one row.
	template <class RESULT_TYPE, class WRITE_FUNCTION>
	static void Execute(DataChunk &input, const ExpressionState &state, Vector &result,
	                    const WRITE_FUNCTION &write_fn) {
		D_ASSERT(input.ColumnCount() == 1 || input.ColumnCount() == 2);
		const auto count = input.size();
		const auto &variant_vec = input.data[0];

		if (input.ColumnCount() == 2) {
			const auto &path = input.data[1];
			D_ASSERT(path.GetVectorType() == VectorType::CONSTANT_VECTOR);
			(void)path;
		}

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.BindInfo()->Cast<VariantPathBindData>();
		auto n_columns = input.ColumnCount();

		if (n_columns == 1) {
			ExecuteUnary<RESULT_TYPE>(variant_vec, {}, result, count, write_fn);
			return;
		}

		D_ASSERT(n_columns == 2);
		const auto &path_type_id = input.data[1].GetType().id();

		if (path_type_id == LogicalTypeId::LIST) {
			ExecuteMany<RESULT_TYPE>(variant_vec, info.paths, result, count, write_fn);
			return;
		}
		D_ASSERT(path_type_id == LogicalTypeId::VARCHAR || path_type_id == LogicalTypeId::BIGINT);
		ExecuteUnary<RESULT_TYPE>(variant_vec, info.paths[0], result, count, write_fn);
	}

private:
	//! Executes a single path and writes RESULT_TYPE per input row.
	template <class RESULT_TYPE, class WRITE_FUNCTION>
	static void ExecuteUnary(const Vector &input, const vector<VariantPathComponent> &path, Vector &result,
	                         const idx_t count, const WRITE_FUNCTION &write_fn) {
		const VectorIterator<VectorVariantType> variants(input);

		result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
		auto row_writer = FlatVector::Writer<RESULT_TYPE>(result, count);

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (!variants.RowIsValid(row_idx)) {
				row_writer.WriteNull();
				continue;
			}

			write_fn(ResolvePath(variants[row_idx], path), row_writer);
		}
	}

	//! Executes a list of paths and writes LIST<RESULT_TYPE> per input row.
	template <class RESULT_TYPE, class WRITE_FUNCTION>
	static void ExecuteMany(const Vector &input, const vector<vector<VariantPathComponent>> &paths, Vector &result,
	                        const idx_t count, const WRITE_FUNCTION &write_fn) {
		const VectorIterator<VectorVariantType> variants(input);
		const auto keys = GetSingleKeys(paths);

		result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
		auto result_writer = FlatVector::Writer<VectorListType<RESULT_TYPE>>(result, count);

		//! The value of the current row at each of the paths
		vector<optional<VariantNode>> selected(paths.size());
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (!variants.RowIsValid(row_idx)) {
				result_writer.WriteNull();
				continue;
			}

			ResolvePaths(variants[row_idx], paths, keys, selected);

			idx_t path_idx = 0;
			for (auto &path_writer : result_writer.WriteList(paths.size())) {
				write_fn(selected[path_idx], path_writer);
				path_idx++;
			}
		}
	}

	//! Returns the key of each path that is a single key lookup, and an empty optional for the other paths
	static vector<optional<string_t>> GetSingleKeys(const vector<vector<VariantPathComponent>> &paths);
	//! Resolves every path for 'root' into 'selected', the single key paths in one pass over the children of the object
	static void ResolvePaths(const VariantNode &root, const vector<vector<VariantPathComponent>> &paths,
	                         const vector<optional<string_t>> &keys, vector<optional<VariantNode>> &selected);

	//! Returns the element at a 0-based index of an ARRAY, negative indexes count from the end. Returns nullopt if the
	//! node is not an ARRAY or the index is out of range.
	static optional<VariantNode> ResolveIndex(const VariantNode &node, int64_t index);

	//! Walks the VARIANT using a path, which may be either (partially) shredded or unshredded. Returns nullopt if the
	//! path is missing in the VARIANT, and the present value with VariantNode otherwise.
	static optional<VariantNode> ResolvePath(VariantNode node, const vector<VariantPathComponent> &path) {
		for (const auto &component : path) {
			if (node.IsNull() || node.IsMissing()) {
				return {};
			}

			if (component.lookup_mode == VariantChildLookupMode::BY_INDEX) {
				auto element = ResolveIndex(node, component.index);
				if (!element) {
					return {};
				}
				node = *element;
				continue;
			}

			if (node.GetTypeId() != LogicalTypeId::STRUCT) {
				return {};
			}

			bool found = false;
			for (const auto &[key, value] : node.GetObjectChildren()) {
				if (key == component.key) {
					node = value;
					found = true;
					break;
				}
			}

			if (!found) {
				return {};
			}
		}

		return node;
	}
};
} // namespace duckdb
