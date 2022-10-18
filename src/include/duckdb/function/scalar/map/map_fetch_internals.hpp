#pragma once

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct MapValuesFunctor {
	static Vector &FetchVector(Vector &map) {
		D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
		return MapVector::GetValues(map);
	}
};

struct MapKeysFunctor {
	static Vector &FetchVector(Vector &map) {
		D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
		return MapVector::GetKeys(map);
	}
};

struct ListEntryFunctor {
	static Vector &FetchVector(Vector &list) {
		D_ASSERT(list.GetType().id() == LogicalTypeId::LIST);
		return ListVector::GetEntry(list);
	}
};

template <class MapInternalsFunctor, class ResultVectorFunctor, bool SET_LIST_SIZE = true>
void MapFetchInternals(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();

	// Fetch the internal vector
	auto &map = args.data[0];
	auto &map_internals = MapInternalsFunctor::FetchVector(map);

	UnifiedVectorFormat map_internals_data;
	UnifiedVectorFormat map_data;
	map_internals.ToUnifiedFormat(count, map_internals_data);
	map.ToUnifiedFormat(count, map_data);

	auto &entries = ResultVectorFunctor::FetchVector(result);

	D_ASSERT(entries.GetType().id() == ListType::GetChildType(map_internals.GetType()).id());

	entries.Reference(ListVector::GetEntry(map_internals));

	if (SET_LIST_SIZE) {
		// Reference the data for the list_entry_t's
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		FlatVector::SetData(result, map_internals_data.data);
		FlatVector::SetValidity(result, map_data.validity);
		auto list_size = ListVector::GetListSize(map_internals);
		ListVector::SetListSize(result, list_size);
	}
	if (map.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		result.Slice(*map_data.sel, count);
	}
}

} // namespace duckdb
