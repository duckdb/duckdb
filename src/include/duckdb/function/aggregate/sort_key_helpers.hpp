//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/sort_key_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/function/create_sort_key.hpp"

namespace duckdb {

struct AggregateSortKeyHelpers {
	template <class STATE, class OP, OrderType ORDER_TYPE = OrderType::ASCENDING, bool IGNORE_NULLS = true>
	static void UnaryUpdate(Vector inputs[], AggregateInputData &input_data, idx_t input_count, Vector &state_vector,
	                        idx_t count) {
		D_ASSERT(input_count == 1);
		auto &input = inputs[0];

		Vector sort_key(LogicalType::BLOB);
		auto modifiers = OrderModifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST);
		CreateSortKeyHelpers::CreateSortKey(input, count, modifiers, sort_key);

		UnifiedVectorFormat idata;
		if (IGNORE_NULLS) {
			input.ToUnifiedFormat(count, idata);
		}

		UnifiedVectorFormat kdata;
		sort_key.ToUnifiedFormat(count, kdata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		auto key_data = UnifiedVectorFormat::GetData<string_t>(kdata);
		auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			const auto sidx = sdata.sel->get_index(i);
			if (IGNORE_NULLS) {
				auto idx = idata.sel->get_index(i);
				if (!idata.validity.RowIsValid(idx)) {
					continue;
				}
			}
			const auto key_idx = kdata.sel->get_index(i);
			auto &state = *states[sidx];
			OP::template Execute<string_t, STATE, OP>(state, key_data[key_idx], input_data);
		}
	}
};

} // namespace duckdb
