//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/ordinality_request_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

enum class OrdinalityType : uint8_t { WITHOUT_ORDINALITY = 0, WITH_ORDINALITY = 1 };

inline void SetOrdinality(DataChunk &chunk, const optional_idx &ordinality_column_idx, const idx_t &ordinality_idx,
                          const idx_t &ordinality) {
	D_ASSERT(ordinality_column_idx.IsValid());
	if (ordinality > 0) {
		constexpr idx_t step = 1;
		chunk.data[ordinality_column_idx.GetIndex()].Sequence(static_cast<int64_t>(ordinality_idx), step, ordinality);
	}
}

} // namespace duckdb
