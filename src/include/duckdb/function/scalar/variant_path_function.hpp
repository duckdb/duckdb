//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/variant_path_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

struct VariantPathFunction {
	//! Executes scalar VARIANT functions that accept an optional constant path argument.
	//! The function may be called as f(VARIANT), f(VARIANT, VARCHAR) or f(VARIANT, VARCHAR[]).
	//! `collect_fn` is run once for each requested path and produces an intermediate result.
	//! `write_fn` writes one row from that intermediate result.
	template <class COLLECTED_TYPE, class RESULT_TYPE, class COLLECT_FUNCTION, class WRITE_FUNCTION>
	static void Execute(DataChunk &input, const ExpressionState &state, Vector &result,
	                    const COLLECT_FUNCTION &collect_fn, const WRITE_FUNCTION &write_fn) {
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
			ExecuteUnary<COLLECTED_TYPE, RESULT_TYPE>(variant_vec, {}, result, count, collect_fn, write_fn);
			return;
		}

		D_ASSERT(n_columns == 2);
		const auto &path_type_id = input.data[1].GetType().id();

		if (path_type_id == LogicalTypeId::VARCHAR) {
			ExecuteUnary<COLLECTED_TYPE, RESULT_TYPE>(variant_vec, info.paths[0], result, count, collect_fn, write_fn);
			return;
		}
		if (path_type_id == LogicalTypeId::LIST) {
			ExecuteMany<COLLECTED_TYPE, RESULT_TYPE>(variant_vec, info.paths, result, count, collect_fn, write_fn);
			return;
		}
	}

private:
	//! Executes a single path and writes RESULT_TYPE per input row.
	template <class COLLECTED_TYPE, class RESULT_TYPE, class COLLECT_FUNCTION, class WRITE_FUNCTION>
	static void ExecuteUnary(const Vector &variant_vec, const vector<VariantPathComponent> &components, Vector &result,
	                         const idx_t count, const COLLECT_FUNCTION &collect_fn, const WRITE_FUNCTION &write_fn) {
		RecursiveUnifiedVectorFormat source_format;
		Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
		const UnifiedVariantVectorData variant(source_format);

		const COLLECTED_TYPE collected = collect_fn(variant, components, count);

		result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
		auto row_writer = FlatVector::Writer<RESULT_TYPE>(result, count);

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (!variant.RowIsValid(row_idx)) {
				row_writer.WriteNull();
				continue;
			}

			write_fn(variant, row_writer, collected, row_idx);
		}
	}

	//! Executes a list of paths and writes LIST<RESULT_TYPE> per input row.
	template <class COLLECTED_TYPE, class RESULT_TYPE, class COLLECT_FUNCTION, class WRITE_FUNCTION>
	static void ExecuteMany(const Vector &variant_vec, const vector<vector<VariantPathComponent>> &paths,
	                        Vector &result, const idx_t count, const COLLECT_FUNCTION &collect_fn,
	                        const WRITE_FUNCTION &write_fn) {
		vector<COLLECTED_TYPE> collected_by_path;
		collected_by_path.reserve(paths.size());

		RecursiveUnifiedVectorFormat source_format;
		Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
		const UnifiedVariantVectorData variant(source_format);

		for (const auto &path : paths) {
			collected_by_path.push_back(collect_fn(variant, path, count));
		}

		result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
		auto result_writer = FlatVector::Writer<VectorListType<RESULT_TYPE>>(result, count);

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (!variant.RowIsValid(row_idx)) {
				result_writer.WriteNull();
				continue;
			}

			auto row_writer = result_writer.WriteList(paths.size());

			idx_t path_idx = 0;
			for (auto &path_writer : row_writer) {
				write_fn(variant, path_writer, collected_by_path[path_idx], row_idx);
				path_idx++;
			}
		}
	}
};
} // namespace duckdb
